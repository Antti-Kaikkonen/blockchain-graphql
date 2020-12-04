import { Block } from "../models/block";
import { Resolver, FieldResolver, Root, Field, ArgsType, Args, Arg, Query } from "type-graphql";
import { types, Client } from "cassandra-driver";
import { Inject } from "typedi";
import { ConfirmedTransaction, ConfirmedTransactionCursor, PaginatedConfirmedTransactionResponse } from "../models/confirmed-transaction";
import { PaginationArgs } from "./pagination-args";


@ArgsType()
class ConfirmedTransactionArgs extends PaginationArgs {

  @Field({nullable: true})
  cursor: ConfirmedTransactionCursor;

}

@Resolver(of => Block)
export class BlockResolver {

  constructor(@Inject("cassandra_client") private client: Client) {
    console.log("BlockResolver constructor");
  }

  @FieldResolver( {complexity: ({ childComplexity, args }) => 100 + args.limit * childComplexity})
  async confirmedTransactions(@Root() block: Block, 

    @Args() {cursor, limit}: ConfirmedTransactionArgs

  ): Promise<PaginatedConfirmedTransactionResponse> {
    let args: any[] = [block.height];
    let query: string = "SELECT * FROM "+block.coin.keyspace+".confirmed_transaction WHERE height=?";
    if (cursor) {
      query += " AND tx_n > ?";
      args = args.concat([cursor.tx_n]);
    }
    let resultSet: types.ResultSet = await this.client.execute(
      query, 
      args, 
      {prepare: true, fetchSize: limit}
    );
    let res: ConfirmedTransaction[] = resultSet.rows.map(row => {
      let tx: ConfirmedTransaction = new ConfirmedTransaction();
      tx.height = row.get('height');
      tx.tx_n = row.get('tx_n');
      tx.txid = row.get("txid");
      tx.coin = block.coin;
      return tx;
    });
    return {
      hasMore: resultSet.pageState !== null,
      items: res
    };
  }

}