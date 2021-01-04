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
    if (transactionOutput.spending_txid === null || transactionOutput.spending_txid === undefined) return null;
    let mempool = transactionOutput.coin.mempool;
    let mempoolTx: MempoolTx = mempool === undefined ? undefined : mempool.txById.get(transactionOutput.spending_txid);
    if (mempoolTx !== undefined) {
      let spending_input: RpcVin = mempoolTx.vin[transactionOutput.spending_index];
      let res: TransactionInput = new TransactionInput();
      res.coinbase = spending_input.coinbase;
      res.scriptsig = spending_input.scriptSig;
      res.sequence = spending_input.sequence;
      res.txid = spending_input.txid;
      res.vout = spending_input.vout
      res.spending_txid = transactionOutput.spending_txid;
      res.spending_index = transactionOutput.spending_index;
      res.coin = transactionOutput.coin;
      return res;
    }

    let args: any[] = [transactionOutput.spending_txid, transactionOutput.spending_index];
    let query: string = "SELECT * FROM "+transactionOutput.coin.keyspace+".transaction_input WHERE spending_txid=? AND spending_index=?";
    let resultSet: types.ResultSet = await this.client.execute(
      query, 
      args, 
      {prepare: true}
    );
    let res: TransactionInput[] = resultSet.rows.map(row => {
      let vin: TransactionInput = new TransactionInput();
      vin.coinbase = row.get('coinbase');
      vin.scriptsig = row.get('scriptsig');
      vin.sequence = row.get('sequence');
      vin.txid = row.get('txid');
      vin.vout = row.get('vout');
      vin.spending_txid = row.get('spending_txid');
      vin.spending_index = row.get('spending_index');
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
      tx.locktime = mempoolTx.locktime;
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
      tx.locktime = row.get('locktime');
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