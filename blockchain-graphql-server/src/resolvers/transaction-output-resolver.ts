import { Resolver, FieldResolver, Root } from "type-graphql";
import { types, Client } from "cassandra-driver";
import { Inject } from "typedi";
import { TransactionInput } from "../models/transaction-input";
import { TransactionOutput } from "../models/transaction-output";
import { Transaction } from "../models/transaction";

@Resolver(of => TransactionOutput)
export class TransactionOutputResolver {

  constructor(@Inject("cassandra_client") private client: Client) {
  }

  @FieldResolver( {complexity: ({ childComplexity, args }) => 100 + childComplexity})
  async spendingInput(@Root() transactionOutput: TransactionOutput, 
  ): Promise<TransactionInput> {
    if (transactionOutput.spending_txid === null || transactionOutput.spending_txid === undefined) return null;
    let args: any[] = [transactionOutput.spending_txid, transactionOutput.spending_index];
    let query: string = "SELECT * FROM dash.transaction_input WHERE spending_txid=? AND spending_index=?";
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
      return vin;
    });
    return res[0];
  }

  @FieldResolver( {complexity: ({ childComplexity, args }) => 100 + childComplexity})
  async transaction(@Root() transactionOutput: TransactionOutput, 
  ): Promise<Transaction> {
    if (transactionOutput.txid === null || transactionOutput.txid === undefined) return null;
    let args: any[] = [transactionOutput.txid];
    let query: string = "SELECT * FROM dash.transaction WHERE txid=?";
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
      return tx;
    });
    return res[0];
  }

}