require("dotenv").config();
const { MongoClient, ServerApiVersion } = require("mongodb");
const fetch = (...args) => import("node-fetch").then(({ default: fetch }) => fetch(...args));

const ETHERSCAN_API_KEY = process.env.ETHERSCAN_API_KEY;
const mongodb_username = process.env.MONGO_DB_USERNAME;
const mongodb_password = process.env.MONGO_DB_PASSWORD;
const mongodb_database = process.env.MONGO_DB_DATABASE;

const depositContract = "0x1Cc9cF5586522c6F483E84A19c3C2B0B6d027bF0";

const getDepositTxs = async (lastUpdatedBlock = 15889442) => {
  let startBlock = Math.max(15889442, lastUpdatedBlock);
  let depositTxs = await fetch(`https://api.etherscan.io/api?module=account&action=txlist&address=${depositContract}&startblock=${startBlock}&sort=asc&apikey=${ETHERSCAN_API_KEY}`);
  depositTxs = await depositTxs.json();
  depositTxs = depositTxs.result.filter((a) => a.isError == "0"); // remove txs with errors
  return depositTxs;
};

const getDepositBundle = async (depositTx, retries = 5) => {
  if (retries < 0) {
    // gonna treat this one as a "No Arb", I dunno, some odd edge case usually - if gets bad will revise
    // e.g. 0xa39713c168b0fb7ae33860e56d72742b140ec50f38869470ff6bc62e4a914da3 is weird
    console.log(`treating ${depositTx.hash} as a no-arb tx`);
    return { bundleType: "No Arb" };
  }
  try {
    const [blockNumber, nodeOperator] = [depositTx.blockNumber, depositTx.from];
    let hash = depositTx.hash; // swaps and arbs will override later

    let normalTxBundle = await fetch(`https://api.etherscan.io/api?module=account&action=txlist&address=${nodeOperator}&startblock=${blockNumber}&endblock=${blockNumber}&sort=desc&apikey=${ETHERSCAN_API_KEY}`);
    normalTxBundle = await normalTxBundle.json();
    normalTxBundle = normalTxBundle.result;

    const txMethods = {
      "0x3876de3a": "deposit",
      "0xd9c9662a": "arb",
      "0xd0e30db0": "mint",
      "0x095ea7b3": "approve",
      "0x7c025200": "swap",
    };
    let txs = [];

    // bundle params
    let time, to, netProfit, txFee, minipoolType, bundleType;
    let bundleTypeFields = {};
    let gasCost = [];
    let gasUsed = [];
    let gasPrice = [];
    let ethOutput = 0;
    let ethInput = 0;

    // if it's a no-arb deposit just exit
    if (normalTxBundle.length === 1) {
      bundleType = "No Arb";
      return { bundleType };
    }

    // if it's an arb, ignore, since it's already covered by the client etherscan query
    // yes this is an afterthought because you can see i'm processing the arb transaction below anyway - not gonna delete it
    const isArb = () => {
      return normalTxBundle.map((tx) => tx.methodId === "0xd9c9662a").reduce((a, b) => a || b, false);
    };
    if (isArb()) {
      bundleType = "Arb Flash Loan";
      return { bundleType };
    }

    await Promise.all(
      normalTxBundle.map(async (bundleTx) => {
        let tx = { ...bundleTx };
        tx["method"] = txMethods[tx.methodId] ? txMethods[tx.methodId] : "Error";
        bundleTypeFields[tx["method"]] = true;

        if (Object.values(txMethods).includes(tx["method"]) && tx["method"] !== "deposit") {
          gasPrice.push(Number(tx.gasPrice));
          gasUsed.push(Number(tx.gasUsed));
          gasCost.push((Number(tx.gasPrice) / 1e18) * Number(tx.gasUsed));
        }

        if (tx["method"] == "deposit") {
          minipoolType = Number(tx.value) / 1e18;
          time = tx.timeStamp;
        }

        if (tx["method"] == "mint") {
          ethInput = Number(tx.value) / 1e18;
        }

        if (tx["method"] == "swap" || tx["method"] == "arb") {
          let profitData = await fetch(`https://api.etherscan.io/api?module=account&action=txlistinternal&txhash=${tx.hash}&apikey=${ETHERSCAN_API_KEY}`);
          profitData = await profitData.json();
          ethOutput = profitData.result.filter((obj) => obj.to.toLowerCase() === nodeOperator.toLowerCase())[0].value;
          ethOutput = Number(ethOutput) / 1e18;
          hash = tx.hash;
        }
        txs.push(tx);
      })
    );
    // classify bundle
    if (bundleTypeFields.arb) {
      bundleType = "Arb Flash Loan";
    } else if (bundleTypeFields.mint && bundleTypeFields.swap) {
      bundleType = "Arb No Flash Loan";
    } else if (bundleTypeFields.mint && !bundleTypeFields.swap) {
      bundleType = "Arb Unrealised Gain";
    } else {
      bundleType = "No Arb";
    }

    to = nodeOperator;
    // get gas used/price/cost (arb || mint+approve+swap)
    gasUsed = gasUsed.reduce((a, b) => a + b, 0);
    gasCost = gasCost.reduce((a, b) => a + b, 0);
    gasPrice = gasPrice.reduce((a, b) => a + b, 0) / gasPrice.length;
    netProfit = ethOutput - ethInput - gasCost;
    if (bundleType === "Arb Unrealised Gain") netProfit = 0;
    txFee = gasCost;

    return { time, hash, to, netProfit, txFee, gasUsed, gasPrice, bundleType, minipoolType, blockNumber };
  } catch (e) {
    console.log(`failed on ${depositTx}, sleeping a bit`, e);
    await new Promise((r) => setTimeout(r, 2000));
    return getDepositBundle(depositTx, retries - 1);
  }
};

const pushToDatabase = async (tx) => {
  const uri = `mongodb+srv://${mongodb_username}:${mongodb_password}@${mongodb_database}.mongodb.net/?retryWrites=true&w=majority`;
  const client = new MongoClient(uri, { useNewUrlParser: true, useUnifiedTopology: true, serverApi: ServerApiVersion.v1 });
  try {
    await client.connect();
    const collection = client.db("main").collection("transactions");
    await collection.replaceOne({ _id: tx.hash }, { ...tx, _id: tx.hash }, { upsert: true });
    console.log(`inserted ${tx.hash}`);
  } finally {
    await client.close();
  }
};

const getMongoDBLatestBlock = async (rebuild = false) => {
  if (rebuild) return 15889442; // First block the rocketArb SC was deployed
  let url = "https://data.mongodb-api.com/app/data-bhzbq/endpoint/data/v1/action/find";
  let options = {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Access-Control-Request-Headers": "*",
      "api-key": process.env.MONGO_DB_API_KEY,
    },
    body: JSON.stringify({
      collection: "transactions",
      database: "main",
      dataSource: "Main",
      limit: 50000,
    }),
  };
  try {
    let docs = await fetch(url, options);
    docs = await docs.json();
    let latestBlock = docs.documents.map((a) => a.blockNumber).reduce((a, b) => Math.max(a, b), 15889442);
    return latestBlock;
  } catch (e) {
    console.log(e);
    return 15889442; // First block the rocketArb SC was deployed
  }
};

const parseTransactions = async () => {
  let latestBlock = await getMongoDBLatestBlock();
  let depositTxs = await getDepositTxs(latestBlock + 1);
  for (const tx of depositTxs) {
    let txData = await getDepositBundle(tx);
    console.log(`processing ${tx.hash}, ${txData.bundleType}`);
    if (txData.bundleType !== "No Arb" && txData.bundleType !== "Arb Flash Loan") {
      await pushToDatabase(txData);
    }
  }
  console.log("Done!");
};

(async () => {
  await parseTransactions();
})();
