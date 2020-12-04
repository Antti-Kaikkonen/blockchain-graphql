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

  @FieldResolver({complexity: ({ childComplexity, args }) => 100 + childComplexity})
  async blockHash(@Root() transaction: ConfirmedTransaction, 
  ): Promise<BlockHash> {
    let args: any[] = [transaction.height];
    let query: string = "SELECT * FROM "+transaction.coin.keyspace+".longest_chain WHERE height=?";
    let resultSet: types.ResultSet = await this.client.execute(
      query, 
      args, 
      {prepare: true}
    );
    let res: BlockHash[] = resultSet.rows.map(row => {
      let b: BlockHash = new BlockHash();
      b.hash = row.get('hash');
      b.height = row.get('height');
      b.coin = transaction.coin;
      return b;
    });
    return res[0];
  }
  
  @FieldResolver({complexity: ({ childComplexity, args }) => 100 + childComplexity})
  async transaction(@Root() transaction: ConfirmedTransaction, 
  ): Promise<Transaction> {
    let args: any[] = [transaction.txid];
    let query: string = "SELECT * FROM "+transaction.coin.keyspace+".transaction WHERE txid=?";
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
      tx.coin = transaction.coin;
      return tx;
    });
    return res[0];
  }

}