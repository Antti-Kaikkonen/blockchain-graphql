import { types } from "cassandra-driver";
import { FieldResolver, Resolver, Root } from "type-graphql";
import { Inject } from "typedi";
import { LimitedCapacityClient } from "../limited-capacity-client";
import { MempoolTx } from "../mempool/mempool";
import { UnconfirmedTransaction } from "../models/unconfirmedl-transaction";
import { Transaction } from "../models/transaction";

@Resolver(of => UnconfirmedTransaction)
export class MempoolTransactionsResolver {


  constructor(@Inject("cassandra_client") private client: LimitedCapacityClient) {
  }

  @FieldResolver(returns => Transaction, {nullable: false, complexity: ({ childComplexity, args }) => 100 + childComplexity})
  async transaction(@Root() rootTx: UnconfirmedTransaction, 
  ): Promise<Transaction> {
    let mempoolTransaction: MempoolTx = rootTx.coin.mempool?.txById.get(rootTx.txid);
    if (mempoolTransaction !== undefined) {
      return mempoolTransaction.toGraphQL(rootTx.coin);
    }
    let args: any[] = [rootTx.txid];
    let query: string = "SELECT * FROM "+rootTx.coin.keyspace+".transaction WHERE txid=?";
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
        coin: rootTx.coin
      }
    });
    return res[0];
  }

}