import { Resolver, FieldResolver, Root, Query, Arg } from "type-graphql";
import { types, Client } from "cassandra-driver";
import { Inject } from "typedi";
import { ConfirmedTransaction, ConfirmedTransactionCursor } from "../models/confirmed-transaction";
import { Transaction } from "../models/transaction";
import { BlockHash } from "../models/block_hash";

@Resolver(of => ConfirmedTransaction)
export class ConfirmedTransactionResolver {

  constructor(@Inject("cassandra_client") private client: Client) {
  }

  @Query(returns => ConfirmedTransaction, {nullable: true, complexity: ({ childComplexity, args }) => 100 + childComplexity})
  async confirmedTransaction(
    @Arg("height") height: number, 
    @Arg("tx_n") tx_n: number): Promise<ConfirmedTransaction> {
    let args: any[] = [height, tx_n];
    let query: string = "SELECT * FROM dash.confirmed_transaction WHERE height=? AND tx_n=?";
    let resultSet: types.ResultSet = await this.client.execute(
      query, 
      args, 
      {prepare: true}
    );
    let res: ConfirmedTransaction[] = resultSet.rows.map(row => {
      let tx: ConfirmedTransaction = new ConfirmedTransaction();
      tx.height = row.get('height');
      tx.tx_n = row.get('tx_n');
      tx.txid = row.get("txid");
      return tx;
    });
    return res[0];
  }

  @FieldResolver({complexity: ({ childComplexity, args }) => 100 + childComplexity})
  async blockHash(@Root() transaction: ConfirmedTransaction, 
  ): Promise<BlockHash> {
    let args: any[] = [transaction.height];
    let query: string = "SELECT * FROM dash.longest_chain WHERE height=?";
    let resultSet: types.ResultSet = await this.client.execute(
      query, 
      args, 
      {prepare: true}
    );
    let res: BlockHash[] = resultSet.rows.map(row => {
      let b: BlockHash = new BlockHash();
      b.hash = row.get('hash');
      b.height = row.get('height');
      return b;
    });
    return res[0];
  }
  
  @FieldResolver({complexity: ({ childComplexity, args }) => 100 + childComplexity})
  async transaction(@Root() transaction: ConfirmedTransaction, 
  ): Promise<Transaction> {
    let args: any[] = [transaction.txid];
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
      tx.fee = row.get("tx_fee")/1e8;
      return tx;
    });
    return res[0];
  }

}