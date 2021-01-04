import { Resolver, FieldResolver, Root, Query, Arg } from "type-graphql";
import { types, Client } from "cassandra-driver";
import { Inject } from "typedi";
import { ConfirmedTransaction, ConfirmedTransactionCursor } from "../models/confirmed-transaction";
import { Transaction } from "../models/transaction";
import { BlockHash } from "../models/block_hash";
import { MempoolBlock, MempoolTx } from "../mempool";
import { LimitedCapacityClient } from "../limited-capacity-client";

@Resolver(of => ConfirmedTransaction)
export class ConfirmedTransactionResolver {

  constructor(
    @Inject("cassandra_client") private client: LimitedCapacityClient
  ) {}

  @FieldResolver({complexity: ({ childComplexity, args }) => 100 + childComplexity})
  async blockHash(@Root() transaction: ConfirmedTransaction, 
  ): Promise<BlockHash> {
    let mempool = transaction.coin.mempool;
    let mempoolBlock: MempoolBlock = mempool === undefined ? undefined : mempool.blockByHeight.get(transaction.height);
    if (mempoolBlock !== undefined) {
      let res: BlockHash = new BlockHash();
      res.hash = mempoolBlock.hash;
      res.height = mempoolBlock.height;
      res.coin = transaction.coin;
      return res;
    }
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
    let mempool = transaction.coin.mempool;
    let mempoolTransaction: MempoolTx = mempool === undefined ? undefined : mempool.txById.get(transaction.txid);
    if (mempoolTransaction !== undefined) {
      return mempoolTransaction.toGraphQL(transaction.coin);
    }
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
      tx.fee = row.get("fee");
      tx.coin = transaction.coin;
      return tx;
    });
    return res[0];
  }

}