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
  
  @FieldResolver(returns => Block, {nullable: false, complexity: ({ childComplexity, args }) => 100 + childComplexity})
  async block(@Root() blockHash: BlockHash, 
  ): Promise<Block> {
    let mempool = blockHash.coin.mempool;
    let mempooBlock = mempool === undefined ? undefined : mempool.blockByHash.get(blockHash.hash);
    if (mempooBlock !== undefined) {
      return <Block> {
        height: mempooBlock.height,
        hash: mempooBlock.hash,
        size: mempooBlock.size,
        version: mempooBlock.version,
        versionHex: mempooBlock.versionHex,
        merkleRoot: mempooBlock.merkleroot,
        time: new Date(mempooBlock.time*1000),
        medianTime: mempooBlock.mediantime,
        nonce: mempooBlock.nonce,
        bits: mempooBlock.bits,
        difficulty: mempooBlock.difficulty,
        chainwork: mempooBlock.chainwork,
        previousBlockHash: mempooBlock.previousblockhash,
        txCount: mempooBlock.tx.length,
        coin: blockHash.coin
      }
    }
    let args: any[] = [blockHash.hash];
    let query: string = "SELECT * FROM "+blockHash.coin.keyspace+".block WHERE hash=?";
    let resultSet: types.ResultSet = await this.client.execute(
      query, 
      args, 
      {prepare: true}
    );
    let res: Block[] = resultSet.rows.map(row => {
      return <Block> {
        height: row.get("height"),
        hash: row.get("hash"),
        size: row.get("size"),
        version: row.get("version"),
        versionHex: row.get("versionhex"),
        merkleRoot: row.get("merkleroot"),
        time: new Date(row.get("time")*1000),
        medianTime: row.get("mediantime"),
        nonce: row.get("nonce"),
        bits: row.get("bits"),
        difficulty: row.get("difficulty"),
        chainwork: row.get("chainwork"),
        previousBlockHash: row.get("previousblockhash"),
        txCount: row.get("tx_count"),
        coin: blockHash.coin
      };
    });
    return res[0];
  }

}