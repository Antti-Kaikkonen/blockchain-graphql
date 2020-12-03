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

  @Query(returns => Block, {nullable: true, complexity: ({ childComplexity, args }) => 100 + childComplexity})
  async block(
    @Arg("hash") hash: string): Promise<Block> {
    let args: any[] = [hash];
    let query: string = "SELECT * FROM block WHERE hash=?";
    let resultSet: types.ResultSet = await this.client.execute(
      query, 
      args, 
      {prepare: true}
    );
    let res: Block[] = resultSet.rows.map(row => {
      let b: Block = new Block();
      b.height = row.get('height');
      b.hash = row.get('hash');
      b.height = row.get('height');
      b.version = row.get('version');
      b.versionhex = row.get("versionhex");
      b.merkleroot = row.get("merkleroot");
      b.time = row.get("time");
      b.mediantime = row.get("mediantime");
      b.nonce = row.get("nonce");
      b.bits = row.get("bits");
      b.difficulty = row.get("difficulty");
      b.chainwork = row.get("chainwork");
      b.previousblockhash = row.get("previousblockhash");
      b.tx_count = row.get("tx_count");
      return b;
    });
    return res[0];
  }  

  @FieldResolver( {complexity: ({ childComplexity, args }) => 100 + args.limit * childComplexity})
  async confirmedTransactions(@Root() block: Block, 

    @Args() {cursor, limit}: ConfirmedTransactionArgs

  ): Promise<PaginatedConfirmedTransactionResponse> {
    let args: any[] = [block.height];
    let query: string = "SELECT * FROM confirmed_transaction WHERE height=?";
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
      return tx;
    });
    return {
      hasMore: resultSet.pageState !== null,
      items: res
    };
  }

}