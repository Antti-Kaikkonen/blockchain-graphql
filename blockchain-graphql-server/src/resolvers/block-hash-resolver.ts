import { Block } from "../models/block";
import { Resolver, FieldResolver, Root } from "type-graphql";
import { types } from "cassandra-driver";
import { Inject } from "typedi";
import { BlockHash } from "../models/block_hash";
import { LimitedCapacityClient } from "../limited-capacity-client";

@Resolver(of => BlockHash)
export class BlockHashResolver {

  constructor(
    @Inject("cassandra_client") private client: LimitedCapacityClient
  ) {}
  
  @FieldResolver( {complexity: ({ childComplexity, args }) => 100 + childComplexity})
  async block(@Root() blockHash: BlockHash, 
  ): Promise<Block> {
    let mempool = blockHash.coin.mempool;
    let mempooBlock = mempool === undefined ? undefined : mempool.blockByHash.get(blockHash.hash);
    if (mempooBlock !== undefined) {
      let b: Block = new Block();
      b.height = mempooBlock.height;
      b.hash = mempooBlock.hash;
      b.size = mempooBlock.size;
      b.height = mempooBlock.height;
      b.version = mempooBlock.version;
      b.versionhex = mempooBlock.versionHex;
      b.merkleroot = mempooBlock.merkleroot;
      b.time = new Date(mempooBlock.time*1000);
      b.mediantime = mempooBlock.mediantime;
      b.nonce = mempooBlock.nonce;
      b.bits = mempooBlock.bits;
      b.difficulty = mempooBlock.difficulty;
      b.chainwork = mempooBlock.chainwork;
      b.previousblockhash = mempooBlock.previousblockhash;
      b.tx_count = mempooBlock.tx.length;
      b.coin = blockHash.coin;
      return b;
    }
    let args: any[] = [blockHash.hash];
    let query: string = "SELECT * FROM "+blockHash.coin.keyspace+".block WHERE hash=?";
    let resultSet: types.ResultSet = await this.client.execute(
      query, 
      args, 
      {prepare: true}
    );
    let res: Block[] = resultSet.rows.map(row => {
      let b: Block = new Block();
      b.height = row.get("height");
      b.hash = row.get("hash");
      b.size = row.get("size");
      b.height = row.get("height");
      b.version = row.get("version");
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
      b.coin = blockHash.coin;
      return b;
    });
    return res[0];
  }

}