import { Resolver, FieldResolver, Root } from "type-graphql";
import { types } from "cassandra-driver";
import { Inject } from "typedi";
import { Transaction } from "../models/transaction";
import { TransactionInput } from "../models/transaction-input";
import { TransactionOutput } from "../models/transaction-output";
import { Address } from "../models/address";
import { ScriptPubKey } from "../models/scriptpubkey";
import { RpcVout } from "../rpc-client";
import { LimitedCapacityClient } from "../limited-capacity-client";

@Resolver(of => TransactionInput)
export class TransactionInputResolver {

  constructor(
    @Inject("cassandra_client") private client: LimitedCapacityClient
  ) {}

  @FieldResolver(returns => TransactionOutput, {nullable: true, complexity: ({ childComplexity, args }) => 100 + childComplexity})
  async spentOutput(@Root() transactionInput: TransactionInput, 
  ): Promise<TransactionOutput> {
    if (transactionInput.txid === null || transactionInput.txid === undefined) return null;
    transactionInput.coin.mempool
    let mempool = transactionInput.coin.mempool;
    let mempoolTx = mempool === undefined ? undefined : mempool.txById.get(transactionInput.txid);
    if (mempoolTx !== undefined) {
      let rpcVout: RpcVout = mempoolTx.vout[transactionInput.vout];
      
      let scriptpubkey: ScriptPubKey = new ScriptPubKey();// = rpcVout.scriptPubKey;
      scriptpubkey.asm = rpcVout.scriptPubKey.asm;
      scriptpubkey.hex = rpcVout.scriptPubKey.hex;
      scriptpubkey.reqSigs = rpcVout.scriptPubKey.reqSigs;
      scriptpubkey.type = rpcVout.scriptPubKey.type;
      if (rpcVout.scriptPubKey.addresses !== undefined && rpcVout.scriptPubKey.addresses !== null) {
        scriptpubkey.addresses = rpcVout.scriptPubKey.addresses.map(address => new Address({address: address, coin: transactionInput.coin}));
      }
      let vout: TransactionOutput = new TransactionOutput({txid: mempoolTx.txid, 
        n: rpcVout.n, 
        value: rpcVout.value, 
        scriptPubKey: scriptpubkey, 
        coin: transactionInput.coin
      });
      let spending_inpoint = mempool === undefined ? undefined : mempool.outpointToInpoint.get(vout.txid+vout.n);
      if (spending_inpoint !== null) {
        vout.spendingTxid = spending_inpoint.spending_txid;
        vout.spendingIndex = spending_inpoint.spending_index;
      }
      return vout;
    }
    let args: any[] = [transactionInput.txid, transactionInput.vout];
    let query: string = "SELECT * FROM "+transactionInput.coin.keyspace+".transaction_output WHERE txid=? AND n=?";
    let resultSet: types.ResultSet = await this.client.execute(
      query, 
      args, 
      {prepare: true}
    );
    let res: TransactionOutput[] = resultSet.rows.map(row => {
      let scriptpubkey = row.get('scriptpubkey');
      if (scriptpubkey.addresses !== undefined && scriptpubkey.addresses !== null) {
        scriptpubkey.addresses = scriptpubkey.addresses.map(address => new Address({address: address, coin: transactionInput.coin}));
      }
      let spendingTxid: string = row.get('spending_txid');
      let spendingIndex: number = row.get('spending_index');
      if (spendingTxid === undefined || spendingTxid === null) {
        let spending_inpoint = mempool === undefined ? undefined : mempool.outpointToInpoint.get(transactionInput.txid+transactionInput.vout);
        if (spending_inpoint !== null) {
          spendingTxid = spending_inpoint.spending_txid;
          spendingIndex = spending_inpoint.spending_index;
        }
      }
      return new TransactionOutput({
        txid: transactionInput.txid,
        n: transactionInput.vout,
        value: row.get('value'),
        scriptPubKey: scriptpubkey,
        spendingTxid: spendingTxid,
        spendingIndex: spendingIndex,
        coin: transactionInput.coin
      });
    });
    return res[0];
  }

  @FieldResolver(returns => Transaction, {nullable: false, complexity: ({ childComplexity, args }) => 100 + childComplexity})
  async transaction(@Root() transactionInput: TransactionInput, 
  ): Promise<Transaction> {
    if (transactionInput.spendingTxid === null || transactionInput.spendingTxid === undefined) return null;
    let mempool = transactionInput.coin.mempool;
    let mempoolTx = mempool === undefined ? undefined : mempool.txById.get(transactionInput.spendingTxid);
    if (mempoolTx !== undefined) {
      return <Transaction> {
        txid: mempoolTx.txid,
        lockTime: mempoolTx.locktime,
        size: mempoolTx.size,
        version: mempoolTx.version,
        height: mempoolTx.height,
        txN: mempoolTx.txN,
        fee: mempoolTx.fee,
        coin: transactionInput.coin
      };
    }
    let args: any[] = [transactionInput.spendingTxid];
    let query: string = "SELECT * FROM "+transactionInput.coin.keyspace+".transaction WHERE txid=?";
    let resultSet: types.ResultSet = await this.client.execute(
      query, 
      args, 
      {prepare: true}
    );
    let res: Transaction[] = resultSet.rows.map(row => {
      return <Transaction> {
        txid: row.get('txid'),
        lockTime: row.get('locktime'),
        size: row.get('size'),
        version: row.get('version'),
        height: row.get('height'),
        txN: row.get("tx_n"),
        fee: row.get("fee"),
        coin: transactionInput.coin
      };
    });
    return res[0];
  }

}