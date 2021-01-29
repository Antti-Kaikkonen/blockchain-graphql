import { Block } from "../models/block";
import { Resolver, FieldResolver, Root, Field, ArgsType, Args } from "type-graphql";
import { types } from "cassandra-driver";
import { Inject } from "typedi";
import { ConfirmedTransaction, ConfirmedTransactionCursor, PaginatedConfirmedTransactionResponse } from "../models/confirmed-transaction";
import { PaginationArgs } from "./pagination-args";
import { MempoolBlock, MempoolTx } from "../mempool";
import { LimitedCapacityClient } from "../limited-capacity-client";


@ArgsType()
class ConfirmedTransactionArgs extends PaginationArgs {

  @Field({nullable: true})
  cursor: ConfirmedTransactionCursor;

}

@Resolver(of => Block)
export class BlockResolver {

  constructor(
    @Inject("cassandra_client") private client: LimitedCapacityClient
  ) {}

  @FieldResolver( {complexity: ({ childComplexity, args }) => 100 + args.limit * childComplexity})
  async confirmedTransactions(@Root() block: Block, 

    @Args() {cursor, limit}: ConfirmedTransactionArgs

  ): Promise<PaginatedConfirmedTransactionResponse> {
    let mempool = block.coin.mempool;
    let mempoolBlock: MempoolBlock = mempool === undefined ? undefined : mempool.blockByHeight.get(block.height);
    if (mempoolBlock !== undefined) {
      let res: ConfirmedTransaction[] = [];
      let fromIndex = cursor === undefined ? 0 : cursor.tx_n+1;
      for (let tx_n = fromIndex; tx_n < mempoolBlock.tx.length; tx_n++) {
        if (res.length == limit) {
          return {
            hasMore: true,
            items: res
          };
        }
        let mempoolTx: MempoolTx = mempoolBlock.tx[tx_n];
        let tx: ConfirmedTransaction = new ConfirmedTransaction();
        tx.height = mempoolTx.height;
        tx.tx_n = mempoolTx.txN;
        tx.txid = mempoolTx.txid;
        tx.coin = block.coin;
        res.push(tx);
      }
      return {
        hasMore: false,
        items: res
      };
    }
    let args: any[] = [block.height];
    let query: string = "SELECT * FROM "+block.coin.keyspace+".confirmed_transaction WHERE height=?";
    if (cursor) {
      query += " AND tx_n > ?";
      args = args.concat([cursor.tx_n]);
    }
    query += " LIMIT ?"
    args.push(limit+1);
    let resultSet: types.ResultSet = await this.client.execute(
      query, 
      args, 
      {prepare: true, fetchSize: null}
    );
    let hasMore: boolean = resultSet.rows.length > limit;
    if (hasMore) resultSet.rows.pop();
    let res: ConfirmedTransaction[] = resultSet.rows.map(row => {
      let tx: ConfirmedTransaction = new ConfirmedTransaction();
      tx.height = row.get('height');
      tx.tx_n = row.get('tx_n');
      tx.txid = row.get("txid");
      tx.coin = block.coin;
      return tx;
    });
    return {
      hasMore: hasMore,
      items: res
    };
  }

}