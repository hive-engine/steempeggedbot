require('dotenv').config();
const nodeCleanup = require('node-cleanup');
const SSC = require('sscjs');
const dsteem = require('dsteem');
const { Queue } = require('./libs/Queue');
const config = require('./config');


const { account, bigWithdrawalsAmount } = config;
const activeKey = dsteem.PrivateKey.from(process.env.ACTIVE_KEY);
const steemNodes = new Queue();
const sscNodes = new Queue();
config.steemNodes.forEach(node => steemNodes.push(node));
config.sscNodes.forEach(node => sscNodes.push(node));

const getSteemNode = () => {
  const node = steemNodes.pop();
  steemNodes.push(node);

  console.log('Using Steem node:', node); // eslint-disable-line no-console
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
const timeout = 3000;
const contractName = 'steempegged';
const contractAction = 'removeWithdrawal';
const tableName = 'withdrawals';

let steem = new dsteem.Client(getSteemNode());
let ssc = new SSC(getSSCNode());

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

// transfer the Steem to the accounts according to the parameters we retrieved from the contract
const transferAssets = async () => {
  const ops = pendingWithdrawals.map(el => buildTranferTx(el));

  try {
    console.log('sending out:', ops); // eslint-disable-line no-console
    await steem.broadcast.sendOperations(ops, activeKey);
  } catch (error) {
    console.error(error); // eslint-disable-line no-console
    steem = new dsteem.Client(getSteemNode());
    await transferAssets(); // try to transfer again
  }
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
      await steem.broadcast.sendOperations(ops, activeKey);
      newTxIDs.forEach((newTx) => {
        bigPendingWithdrawalsIDs.push(newTx);
      });
    }
  } catch (error) {
    console.error(error); // eslint-disable-line no-console
    steem = new dsteem.Client(getSteemNode());
    await processBigPendingWithdrawals(transactions); // try to transfer again
  }
};

// get the pending withdrawals from the 'withdrawals' table of the smart contract
const getPendingWithdrawals = async () => {
  pendingWithdrawals = [];
  pendingWithdrawals = await ssc.find(contractName, tableName, {
    quantity: {
      $lt: bigWithdrawalsAmount,
    },
  }, maxNumberPendingWithdrawals);

  if (pendingWithdrawals.length <= 0) {
    setTimeout(() => getPendingWithdrawals(), timeout);
  } else {
    processPendingWithdrawals();
  }
};

const getBigPendingWithdrawals = async () => {
  let bigPendingWithdrawals = [];
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
    ssc = new SSC(getSSCNode());
  }
};

// start polling the pending withdrawals
getPendingWithdrawals();

// start polling the BIG pending withdrawals
getBigPendingWithdrawals();

// graceful app closing
nodeCleanup((exitCode, signal) => { // eslint-disable-line no-unused-vars
  console.log('closing app'); // eslint-disable-line no-console
});
