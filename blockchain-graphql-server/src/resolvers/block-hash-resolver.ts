import { Block } from "../models/block";
import { Resolver, FieldResolver, Root, Arg, Query } from "type-graphql";
import { types, Client } from "cassandra-driver";
import { Inject } from "typedi";
import { BlockHash } from "../models/block_hash";

@Resolver(of => BlockHash)
export class BlockHashResolver {

  constructor(@Inject("cassandra_client") private client: Client) {
  }

  @Query(returns => BlockHash, {nullable: true, complexity: ({ childComplexity, args }) => 100 + childComplexity})
  async blockHash(
    @Arg("height") height: number): Promise<BlockHash> {
    let args: any[] = [height];
    let query: string = "SELECT * FROM longest_chain WHERE height=?";
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

  @FieldResolver( {complexity: ({ childComplexity, args }) => 100 + childComplexity})
  async block(@Root() blockHash: BlockHash, 
  ): Promise<Block> {
    let args: any[] = [blockHash.hash];
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
      b.time = new Date(row.get("time")*1000);
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

}