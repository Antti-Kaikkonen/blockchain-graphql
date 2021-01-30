import { Resolver, FieldResolver, Root } from "type-graphql";
import { types } from "cassandra-driver";
import { Inject } from "typedi";
import { TransactionInput } from "../models/transaction-input";
import { TransactionOutput } from "../models/transaction-output";
import { Transaction } from "../models/transaction";
import { MempoolTx } from "../mempool";
import { RpcVin } from "../rpc-client";
import { LimitedCapacityClient } from "../limited-capacity-client";

@Resolver(of => TransactionOutput)
export class TransactionOutputResolver {

  constructor(
    @Inject("cassandra_client") private client: LimitedCapacityClient
  ) {}

  @FieldResolver( {complexity: ({ childComplexity, args }) => 100 + childComplexity})
  async spendingInput(@Root() transactionOutput: TransactionOutput, 
  ): Promise<TransactionInput> {
    if (transactionOutput.spendingTxid === null || transactionOutput.spendingTxid === undefined) return null;
    let mempool = transactionOutput.coin.mempool;
    let mempoolTx: MempoolTx = mempool === undefined ? undefined : mempool.txById.get(transactionOutput.spendingTxid);
    if (mempoolTx !== undefined) {
      let spending_input: RpcVin = mempoolTx.vin[transactionOutput.spendingIndex];
      let res: TransactionInput = new TransactionInput();
      res.coinbase = spending_input.coinbase;
      res.scriptSig = spending_input.scriptSig;
      res.sequence = spending_input.sequence;
      res.txid = spending_input.txid;
      res.vout = spending_input.vout
      res.spendingTxid = transactionOutput.spendingTxid;
      res.spendingIndex = transactionOutput.spendingIndex;
      res.coin = transactionOutput.coin;
      return res;
    }

    let args: any[] = [transactionOutput.spendingTxid, transactionOutput.spendingIndex];
    let query: string = "SELECT * FROM "+transactionOutput.coin.keyspace+".transaction_input WHERE spending_txid=? AND spending_index=?";
    let resultSet: types.ResultSet = await this.client.execute(
      query, 
      args, 
      {prepare: true}
    );
    let res: TransactionInput[] = resultSet.rows.map(row => {
      let vin: TransactionInput = new TransactionInput();
      vin.coinbase = row.get('coinbase');
      vin.scriptSig = row.get('scriptsig');
      vin.sequence = row.get('sequence');
      vin.txid = row.get('txid');
      vin.vout = row.get('vout');
      vin.spendingTxid = row.get('spending_txid');
      vin.spendingIndex = row.get('spending_index');
      vin.coin = transactionOutput.coin;
      return vin;
    });
    return res[0];
  }

  @FieldResolver( {complexity: ({ childComplexity, args }) => 100 + childComplexity})
  async transaction(@Root() transactionOutput: TransactionOutput, 
  ): Promise<Transaction> {
    if (transactionOutput.txid === null || transactionOutput.txid === undefined) return null;
    let mempool = transactionOutput.coin.mempool;
    let mempoolTx = mempool === undefined ? undefined : mempool.txById.get(transactionOutput.txid);
    if (mempoolTx !== undefined) {
      let tx: Transaction = new Transaction();
      tx.txid = mempoolTx.txid;
      tx.lockTime = mempoolTx.locktime;
      tx.size = mempoolTx.size;
      tx.version = mempoolTx.version;
      tx.height = mempoolTx.height;
      tx.txN = mempoolTx.txN;
      tx.fee = mempoolTx.fee;
      tx.coin = transactionOutput.coin;
      return tx;
    }
    let args: any[] = [transactionOutput.txid];
    let query: string = "SELECT * FROM "+transactionOutput.coin.keyspace+".transaction WHERE txid=?";
    let resultSet: types.ResultSet = await this.client.execute(
      query, 
      args, 
      {prepare: true}
    );
    let res: Transaction[] = resultSet.rows.map(row => {
      let tx: Transaction = new Transaction();
      tx.txid = row.get('txid');
      tx.lockTime = row.get('locktime');
      tx.size = row.get('size');
      tx.version = row.get('version');
      tx.height = row.get('height');
      tx.txN = row.get("tx_n");
      tx.fee = row.get("fee");
      tx.coin = transactionOutput.coin;
      return tx;
    });
    return res[0];
  }

}