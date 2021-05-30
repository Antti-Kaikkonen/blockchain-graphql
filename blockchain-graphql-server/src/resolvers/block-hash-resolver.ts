import { Block } from '../models/block'
import { Resolver, FieldResolver, Root } from 'type-graphql'
import { types } from 'cassandra-driver'
import { Inject } from 'typedi'
import { BlockHash } from '../models/block_hash'
import { LimitedCapacityClient } from '../limited-capacity-client'

@Resolver(of => BlockHash)
export class BlockHashResolver {

    constructor(
        @Inject('cassandra_client') private client: LimitedCapacityClient
    ) { }

    @FieldResolver(returns => Block, { nullable: false, complexity: ({ childComplexity, args }) => 100 + childComplexity })
    async block(@Root() blockHash: BlockHash,
    ): Promise<Block> {
        const mempooBlock = blockHash.coin.mempool?.blockByHash.get(blockHash.hash)
        if (mempooBlock !== undefined) {
            return <Block>{
                height: mempooBlock.rpcBlock.height,
                hash: mempooBlock.rpcBlock.hash,
                size: mempooBlock.rpcBlock.size,
                version: mempooBlock.rpcBlock.version,
                versionHex: mempooBlock.rpcBlock.versionHex,
                merkleRoot: mempooBlock.rpcBlock.merkleroot,
                time: new Date(mempooBlock.rpcBlock.time * 1000),
                medianTime: mempooBlock.rpcBlock.mediantime,
                nonce: mempooBlock.rpcBlock.nonce,
                bits: mempooBlock.rpcBlock.bits,
                difficulty: mempooBlock.rpcBlock.difficulty,
                chainWork: mempooBlock.rpcBlock.chainwork,
                previousBlockHash: mempooBlock.rpcBlock.previousblockhash,
                txCount: mempooBlock.tx.length,
                coin: blockHash.coin
            }
        }
        const args: any[] = [blockHash.hash]
        const query: string = 'SELECT * FROM ' + blockHash.coin.keyspace + '.block WHERE hash=?'
        const resultSet: types.ResultSet = await this.client.execute(
            query,
            args,
            { prepare: true }
        )
        const res: Block[] = resultSet.rows.map(row => {
            return <Block>{
                height: row.get('height'),
                hash: row.get('hash'),
                size: row.get('size'),
                version: row.get('version'),
                versionHex: row.get('versionhex'),
                merkleRoot: row.get('merkleroot'),
                time: new Date(row.get('time') * 1000),
                medianTime: row.get('mediantime'),
                nonce: row.get('nonce'),
                bits: row.get('bits'),
                difficulty: row.get('difficulty'),
                chainWork: row.get('chainwork'),
                previousBlockHash: row.get('previousblockhash'),
                txCount: row.get('tx_count'),
                coin: blockHash.coin
            }
        })
        return res[0]
    }

}