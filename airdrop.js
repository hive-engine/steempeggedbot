require('dotenv').config();
const nodeCleanup = require('node-cleanup');
const LineByLineReader = require('line-by-line');
const fs = require('fs');
const steem = require('steem-js-patched');
const { Queue } = require('./libs/Queue');
const config = require('./config1');


const { account } = config;
//const activeKey = dsteem.PrivateKey.from(process.env.ACTIVE_KEY);
const steemNodes = new Queue();
config.steemNodes.forEach(node => steemNodes.push(node));

const getSteemNode = () => {
  const node = steemNodes.pop();
  steemNodes.push(node);

  console.log('Using Steem node:', node); // eslint-disable-line no-console
  return node;
};

const queryTimeout = 1000;
const node = getSteemNode();
//const steem = new dsteem.Client(node, { timeout: queryTimeout });
steem.api.setOptions({ url: node });

const buildTransferOp = (to, amount, memo) => ['transfer',
  {
    from: account,
    to,
    amount,
    memo,
  },
];

const buildTranferTx = (recipient, quantity, asset) => {
  const memo = 'airdrop for STEEMP holdings';

  return buildTransferOp(recipient, `${quantity} ${asset}`, memo);
};

// transfer the Steem to the accounts according to the parameters we retrieved from the contract
const transferAssets = async (recipient, quantity, asset, callback) => {
  steem.broadcast.transfer(process.env.ACTIVE_KEY, account, recipient, `${quantity} ${asset}`, 'airdrop for STEEMP holdings', (err, result) => {
    if (err) {
      console.log(err);
      fs.appendFileSync('./airdrop.log', `error ${recipient} ${quantity} ${err}\n`);
    } else {
      console.log(result);
      fs.appendFileSync('./airdrop.log', `ok ${recipient} ${quantity}\n`);
    }

    callback();
  });
};

const start = async () => {
  // read the file from the start
  const lr = new LineByLineReader('./airdrop.txt');

  lr.on('line', async (line) => {
    lr.pause();
    if (line !== '') {
      console.log(line);
      const airdrop = line.split(' ');
      const recipient = airdrop[0];
      let quantity = airdrop[1].split('.');
      quantity = `${quantity[0]}.${quantity[1].substring(0, 3)}`;
      if (parseFloat(quantity) >= 0.001) {
        transferAssets(recipient, quantity, 'HIVE', () => {
          setTimeout(() => lr.resume(), 3000);
          //lr.resume()
        });
      } else {
        console.log('skipping', recipient, airdrop[1]);
        fs.appendFileSync('./airdrop.log', `skipped ${recipient} ${airdrop[1]}\n`);
      }
    }
  });

  lr.on('error', (error) => {
    console.error(error);
  });

  lr.on('end', () => {
    console.log('Airdrop done');
  });
};

start();

// start polling the BIG pending withdrawals
// getBigPendingWithdrawals();

// graceful app closing
nodeCleanup((exitCode, signal) => { // eslint-disable-line no-unused-vars
  console.log('closing app'); // eslint-disable-line no-console
});
