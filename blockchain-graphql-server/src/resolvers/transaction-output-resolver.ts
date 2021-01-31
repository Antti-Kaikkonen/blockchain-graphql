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

  @FieldResolver(returns => TransactionInput, {nullable: true, complexity: ({ childComplexity, args }) => 100 + childComplexity})
  async spendingInput(@Root() transactionOutput: TransactionOutput, 
  ): Promise<TransactionInput> {
    if (transactionOutput.spendingTxid === null || transactionOutput.spendingTxid === undefined) return null;
    let mempoolTx: MempoolTx = transactionOutput.coin.mempool?.txById.get(transactionOutput.spendingTxid);
    if (mempoolTx !== undefined) {
      let spending_input: RpcVin = mempoolTx.vin[transactionOutput.spendingIndex];
      let vin: TransactionInput = new TransactionInput({
        coinbase: spending_input.coinbase, 
        scriptSig: spending_input.scriptSig, 
        sequence: spending_input.sequence, 
        txid: spending_input.txid,
        vout: spending_input.vout, 
        spendingTxid: transactionOutput.spendingTxid,
        spendingIndex: transactionOutput.spendingIndex,
        coin: transactionOutput.coin
      });
      return vin;
    }

    let args: any[] = [transactionOutput.spendingTxid, transactionOutput.spendingIndex];
    let query: string = "SELECT * FROM "+transactionOutput.coin.keyspace+".transaction_input WHERE spending_txid=? AND spending_index=?";
    let resultSet: types.ResultSet = await this.client.execute(
      query, 
      args, 
      {prepare: true}
    );
    let res: TransactionInput[] = resultSet.rows.map(row => {
      let vin: TransactionInput = new TransactionInput({
        coinbase: row.get("coinbase"), 
        scriptSig: row.get("scriptsig"), 
        sequence: row.get('sequence'), 
        txid: row.get('txid'),
        vout: row.get('vout'), 
        spendingTxid: row.get('spending_txid'),
        spendingIndex: row.get('spending_index'),
        coin: transactionOutput.coin
      });
      return vin;
    });
    return res[0];
  }

  @FieldResolver(returns => Transaction, {nullable: false, complexity: ({ childComplexity, args }) => 100 + childComplexity})
  async transaction(@Root() transactionOutput: TransactionOutput, 
  ): Promise<Transaction> {
    if (transactionOutput.txid === null || transactionOutput.txid === undefined) return null;
    let mempoolTx = transactionOutput.coin.mempool?.txById.get(transactionOutput.txid);
    if (mempoolTx !== undefined) {
      return <Transaction> {
        txid: mempoolTx.txid,
        lockTime: mempoolTx.locktime,
        size: mempoolTx.size,
        version: mempoolTx.version,
        height: mempoolTx.height,
        txN: mempoolTx.txN,
        fee: mempoolTx.fee,
        coin: transactionOutput.coin
      };
    }
    let args: any[] = [transactionOutput.txid];
    let query: string = "SELECT * FROM "+transactionOutput.coin.keyspace+".transaction WHERE txid=?";
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
        coin: transactionOutput.coin
      };
    });
    return res[0];
  }

}