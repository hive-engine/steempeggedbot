require('dotenv').config();
const nodeCleanup = require('node-cleanup');
const SSC = require('sscjs');
const dhive = require('@hiveio/dhive');
const { Queue } = require('./libs/Queue');
const config = require('./config');


const { account, bigWithdrawalsAmount } = config;
const isFeeHandler = parseInt(config.handleFees, 10) === 1 ? true : false;
const apiVerificationsNeeded = parseInt(config.apiVerificationsNeeded, 10);
const activeKey = dhive.PrivateKey.from(process.env.ACTIVE_KEY);
const steemNodes = new Queue();
const sscNodes = new Queue();
config.steemNodes.forEach(node => steemNodes.push(node));
config.sscNodes.forEach(node => sscNodes.push(node));

if (isFeeHandler) {
  console.log('Configured to process fee transactions');
}

const getSteemNode = () => {
  const node = steemNodes.pop();
  steemNodes.push(node);

  console.log('Using HIVE node:', node); // eslint-disable-line no-console
  return node;
};

const getSSCNode = () => {
  const node = sscNodes.pop();
  sscNodes.push(node);

  console.log('Using SSC node:', node); // eslint-disable-line no-console
  return node;
};

let pendingWithdrawals = [];
const bigPendingWithdrawalsIDs = new Queue(1000);
const maxNumberPendingWithdrawals = 10;
const timeout = 500;
const queryTimeout = 3000;
const contractName = 'hivepegged';
const contractAction = 'removeWithdrawal';
const tableName = 'withdrawals';

let hive = new dhive.Client(getSteemNode(), { timeout: queryTimeout });
let ssc = new SSC(getSSCNode(), queryTimeout);

const wait = milliseconds => new Promise(res => setTimeout(() => res(), milliseconds));

const buildTransferOp = (to, amount, memo) => ['transfer',
  {
    from: account,
    to,
    amount,
    memo,
  },
];

const buildTranferTx = (tx) => {
  const {
    id,
    type,
    recipient,
    quantity,
  } = tx;

  const memo = {
    id: config.chainId,
    json: {
      contractName,
      contractAction,
      contractPayload: {
        id,
      },
    },
  };

  return buildTransferOp(recipient, `${quantity} ${type}`, JSON.stringify(memo));
};

const isTrxOfInterest = (txId) => {
  let isFirstTestPassed = false;
  if ((isFeeHandler && txId.indexOf('fee') >= 0) || (!isFeeHandler && txId.indexOf('fee') < 0) {
    isFirstTestPassed = true;
  }
  return isFirstTestPassed;
}

const isTrxVerified = async (txId) => {
  const id = txId.split('-fee')[0];
  console.log(`verifying: ${txId}`);

  const txInfo = await ssc.getTransactionInfo(id);
  if (txInfo) {
    const blockNum = txInfo.blockNumber;
    console.log(`${txId}: found in block ${blockNum}`);

    let confirms = 0;
    let nodeList = [];
    let round = null;
    let roundHash = null;
    let witness = null;
    let signingKey = null;
    let roundSignature = null;
    while (confirms < apiVerificationsNeeded) {
      let nextNode = '';
      try {
        nextNode = getSSCNode();
        if (nodeList.includes(nextNode)) {
          continue; // need to make sure we query different nodes each time
        }
        ssc = new SSC(nextNode, queryTimeout);
        let blockInfo = await ssc.getBlockInfo(blockNum);

        if (!blockInfo.round || !blockInfo.witness || blockInfo.witness.length === 0
          || !blockInfo.roundHash || blockInfo.roundHash.length === 0
          || !blockInfo.signingKey || blockInfo.signingKey.length === 0
          || !blockInfo.roundSignature || blockInfo.roundSignature.length === 0) {
          console.log(`${txId}: not verified yet; skipping for now`);
          await wait(timeout);
          return false;
	}

        if (nodeList.length === 0) {
          round = blockInfo.round;
          roundHash = blockInfo.roundHash;
          witness = blockInfo.witness;
          signingKey = blockInfo.signingKey;
          roundSignature = blockInfo.roundSignature;
        } else if (blockInfo.round !== round || blockInfo.roundHash !== roundHash
          || blockInfo.witness !== witness || blockInfo.signingKey !== signingKey
          || blockInfo.roundSignature !== roundSignature) {
	  console.log(`${txId}: verification mismatch on ${nextNode}, num confirms: ${confirms}, checked nodes: [${nodeList}]`);
          await wait(timeout);
          return false;
        }

        nodeList.push(nextNode);
        confirms += 1;
        console.log(`${txId}: verified by ${nextNode} (confirms: ${confirms} of ${apiVerificationsNeeded})`);
      } catch (error) {
        console.log(error);
      }
      await wait(timeout);
    }
    console.log(`${txId}: verified by ${confirms} different nodes`);
    return true;
  }

  return false;
};

// transfer the Steem to the accounts according to the parameters we retrieved from the contract
const transferAssets = async () => {
  const ops = pendingWithdrawals.map(el => buildTranferTx(el));

  // try {
  console.log('sending out:', ops); // eslint-disable-line no-console
  await hive.broadcast.sendOperations(ops, activeKey);
  /* } catch (error) {
    console.error(error); // eslint-disable-line no-console
    hive = new dhive.Client(getSteemNode(), { timeout: queryTimeout });
    await transferAssets(); // try to transfer again
  } */
};

// transfer the pending withdrawals according to what we retrieved from the smart contract
const processPendingWithdrawals = async () => {
  // generate Steem transfers and send the funds out
  await transferAssets();

  // check status
  checkWithdrawalsStatus(); // eslint-disable-line no-use-before-define
};

// send a notification to the account that requested the withdrawal
const processBigPendingWithdrawals = async (transactions) => {
  const ops = [];
  const newTxIDs = [];
  transactions.forEach((tx) => {
    const {
      id,
      recipient,
    } = tx;

    if (!bigPendingWithdrawalsIDs.includes(id)) {
      const memo = `Large withdrawals need to go through a manual review process for security purposes. Please contact us on the Steem Engine Discord (https://discord.gg/GgqYDb7) for questions or concerns. (tx: ${id})`;
      ops.push(buildTransferOp(recipient, '0.001 STEEM', memo));
      newTxIDs.push(id);
    }
  });

  try {
    if (ops.length > 0) {
      console.log('sending out notification:', ops); // eslint-disable-line no-console
      await hive.broadcast.sendOperations(ops, activeKey);
      newTxIDs.forEach((newTx) => {
        bigPendingWithdrawalsIDs.push(newTx);
      });
    }
  } catch (error) {
    console.error(error); // eslint-disable-line no-console
    hive = new dhive.Client(getSteemNode(), { timeout: queryTimeout });
    await processBigPendingWithdrawals(transactions); // try to transfer again
  }
};

// get the pending withdrawals from the 'withdrawals' table of the smart contract
const getPendingWithdrawals = async () => {
  pendingWithdrawals = [];
  try {
    const res = await ssc.find(contractName, tableName, { }, maxNumberPendingWithdrawals);
    for (let index = 0; index < res.length; index += 1) {
      const element = res[index];
      if (isTrxOfInterest(element.id)) {
        const isSafeTrx = await isTrxVerified(element.id);
        if (isSafeTrx && parseFloat(element.quantity) < parseFloat(config.bigWithdrawalsAmount)) {
          pendingWithdrawals.push(element);
          break;
        }
      }
    }

    if (pendingWithdrawals.length <= 0) {
      setTimeout(() => getPendingWithdrawals(), timeout);
    } else {
      processPendingWithdrawals();
    }
  } catch (error) {
    console.log(error);
    ssc = new SSC(getSSCNode(), queryTimeout);
    setTimeout(() => getPendingWithdrawals(), timeout);
  }
};

const getBigPendingWithdrawals = async () => {
  let bigPendingWithdrawals = [];
  try {
    bigPendingWithdrawals = await ssc.find(contractName, tableName, {
      quantity: {
        $gte: bigWithdrawalsAmount,
      },
    });

    if (bigPendingWithdrawals.length <= 0) {
      setTimeout(() => getBigPendingWithdrawals(), timeout);
    } else {
      processBigPendingWithdrawals(bigPendingWithdrawals);
    }
  } catch (error) {
    console.log(error);
    ssc = new SSC(getSSCNode(), queryTimeout);
    setTimeout(() => getBigPendingWithdrawals(), timeout);
  }
};

// check if the pending withdrawals "payments" have been processed by the sidechain
const checkWithdrawalsStatus = async () => {
  const txIds = pendingWithdrawals.map(el => el.id);

  try {
    const res = await ssc.find(contractName, tableName, { id: { $in: txIds } });

    if (res.length <= 0) {
      getPendingWithdrawals();
    } else {
      setTimeout(() => checkWithdrawalsStatus(), timeout);
    }
  } catch (error) {
    console.log(error);
    ssc = new SSC(getSSCNode(), queryTimeout);
    setTimeout(() => checkWithdrawalsStatus(), timeout);
  }
};

// start polling the pending withdrawals
getPendingWithdrawals();

// start polling the BIG pending withdrawals
// getBigPendingWithdrawals();

// graceful app closing
nodeCleanup((exitCode, signal) => { // eslint-disable-line no-unused-vars
  console.log('closing app'); // eslint-disable-line no-console
});
