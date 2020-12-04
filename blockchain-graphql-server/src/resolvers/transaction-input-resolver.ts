import { Resolver, FieldResolver, Root } from "type-graphql";
import { types, Client } from "cassandra-driver";
import { Inject } from "typedi";
import { Transaction } from "../models/transaction";
import { TransactionInput } from "../models/transaction-input";
import { TransactionOutput } from "../models/transaction-output";
import { Address } from "../models/address";

@Resolver(of => TransactionInput)
export class TransactionInputResolver {

  constructor(@Inject("cassandra_client") private client: Client) {
  }

  @FieldResolver( {complexity: ({ childComplexity, args }) => 100 + childComplexity})
  async spentOutput(@Root() transactionInput: TransactionInput, 
  ): Promise<TransactionOutput> {
    if (transactionInput.txid === null || transactionInput.txid === undefined) return null;
    let args: any[] = [transactionInput.txid, transactionInput.vout];
    let query: string = "SELECT * FROM "+transactionInput.coin.keyspace+".transaction_output WHERE txid=? AND n=?";
    let resultSet: types.ResultSet = await this.client.execute(
      query, 
      args, 
      {prepare: true}
    );
    let res: TransactionOutput[] = resultSet.rows.map(row => {
      let vout: TransactionOutput = new TransactionOutput();
      vout.txid = row.get('txid');
      vout.n = row.get('n');
      vout.value = row.get('value');
      let scriptpubkey = row.get('scriptpubkey');
      if (scriptpubkey.addresses !== undefined && scriptpubkey.addresses !== null) {
        scriptpubkey.addresses = scriptpubkey.addresses.map(address => new Address(address, transactionInput.coin));
      }
      vout.scriptpubkey = scriptpubkey;
      vout.spending_txid = row.get('spending_txid');
      vout.spending_index = row.get('spending_index');
      vout.coin = vout.coin;
      return vout;
    });
    return res[0];
  }

  @FieldResolver( {complexity: ({ childComplexity, args }) => 100 + childComplexity})
  async transaction(@Root() transactionInput: TransactionInput, 
  ): Promise<Transaction> {
    if (transactionInput.spending_txid === null || transactionInput.spending_txid === undefined) return null;
    let args: any[] = [transactionInput.spending_txid];
    let query: string = "SELECT * FROM "+transactionInput.coin.keyspace+".transaction WHERE txid=?";
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
      tx.coin = transactionInput.coin;
      return tx;
    });
    return res[0];
  }

}