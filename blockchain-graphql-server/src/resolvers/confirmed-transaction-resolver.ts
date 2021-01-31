import { Resolver, FieldResolver, Root } from "type-graphql";
import { types } from "cassandra-driver";
import { Inject } from "typedi";
import { ConfirmedTransaction } from "../models/confirmed-transaction";
import { Transaction } from "../models/transaction";
import { BlockHash } from "../models/block_hash";
import { MempoolBlock, MempoolTx } from "../mempool";
import { LimitedCapacityClient } from "../limited-capacity-client";

@Resolver(of => ConfirmedTransaction)
export class ConfirmedTransactionResolver {

  constructor(
    @Inject("cassandra_client") private client: LimitedCapacityClient
  ) {}

  @FieldResolver(returns => BlockHash, {nullable: false, complexity: ({ childComplexity, args }) => 100 + childComplexity})
  async blockHash(@Root() transaction: ConfirmedTransaction, 
  ): Promise<BlockHash> {
    let mempoolBlock: MempoolBlock = transaction.coin.mempool?.blockByHeight.get(transaction.height);
    if (mempoolBlock !== undefined) {
      return <BlockHash> {
        hash: mempoolBlock.hash,
        height: mempoolBlock.height,
        coin: transaction.coin
      }
    }
    let args: any[] = [transaction.height];
    let query: string = "SELECT * FROM "+transaction.coin.keyspace+".longest_chain WHERE height=?";
    let resultSet: types.ResultSet = await this.client.execute(
      query, 
      args, 
      {prepare: true}
    );
    let res: BlockHash[] = resultSet.rows.map(row => {
      return <BlockHash> {
        hash: row.get('hash'),
        height: row.get('height'),
        coin: transaction.coin,
      }
    });
    return res[0];
  }
  
  @FieldResolver(returns => Transaction, {nullable: false, complexity: ({ childComplexity, args }) => 100 + childComplexity})
  async transaction(@Root() transaction: ConfirmedTransaction, 
  ): Promise<Transaction> {
    let mempoolTransaction: MempoolTx = transaction.coin.mempool?.txById.get(transaction.txid);
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
      return <Transaction> {
        txid: row.get('txid'),
        lockTime: row.get('locktime'),
        size: row.get('size'),
        version: row.get('version'),
        height: row.get('height'),
        txN: row.get("tx_n"),
        fee: row.get("fee"),
        coin: transaction.coin
      }
    });
    return res[0];
  }

}