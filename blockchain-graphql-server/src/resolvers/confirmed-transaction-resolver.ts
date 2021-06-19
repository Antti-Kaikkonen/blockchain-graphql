import { Resolver, FieldResolver, Root } from 'type-graphql'
import { types } from 'cassandra-driver'
import { Inject } from 'typedi'
import { ConfirmedTransaction } from '../models/confirmed-transaction'
import { Transaction } from '../models/transaction'
import { BlockHash } from '../models/block_hash'
import { MempoolBlock, MempoolTx } from '../mempool/mempool'
import { LimitedCapacityClient } from '../limited-capacity-client'

@Resolver(of => ConfirmedTransaction)
export class ConfirmedTransactionResolver {

    constructor(
        @Inject('cassandra_client') private client: LimitedCapacityClient
    ) { }

    @FieldResolver(returns => BlockHash, { nullable: false, complexity: ({ childComplexity, args }) => 100 + childComplexity })
    async blockHash(@Root() transaction: ConfirmedTransaction,
    ): Promise<BlockHash> {
        const mempoolBlock: MempoolBlock = transaction.coin.mempool?.blockByHeight.get(transaction.height)
        if (mempoolBlock !== undefined) {
            return <BlockHash>{
                hash: mempoolBlock.rpcBlock.hash,
                height: mempoolBlock.rpcBlock.height,
                coin: transaction.coin
            }
        }
        const args: unknown[] = [transaction.height]
        const query: string = 'SELECT * FROM ' + transaction.coin.keyspace + '.longest_chain WHERE height=?'
        const resultSet: types.ResultSet = await this.client.execute(
            query,
            args,
            { prepare: true }
        )
        const res: BlockHash[] = resultSet.rows.map(row => {
            return <BlockHash>{
                hash: row.get('hash'),
                height: row.get('height'),
                coin: transaction.coin,
            }
        })
        return res[0]
    }

    @FieldResolver(returns => Transaction, { nullable: false, complexity: ({ childComplexity, args }) => 100 + childComplexity })
    async transaction(@Root() transaction: ConfirmedTransaction,
    ): Promise<Transaction> {
        const mempoolTransaction: MempoolTx = transaction.coin.mempool?.txById.get(transaction.txid)
        if (mempoolTransaction !== undefined) {
            return mempoolTransaction.toGraphQL(transaction.coin)
        }
        const args: unknown[] = [transaction.txid]
        const query: string = 'SELECT * FROM ' + transaction.coin.keyspace + '.transaction WHERE txid=?'
        const resultSet: types.ResultSet = await this.client.execute(
            query,
            args,
            { prepare: true }
        )
        const res: Transaction[] = resultSet.rows.map(row => {
            return <Transaction>{
                txid: row.get('txid'),
                lockTime: row.get('locktime'),
                size: row.get('size'),
                version: row.get('version'),
                height: row.get('height'),
                txN: row.get('tx_n'),
                fee: row.get('fee'),
                inputCount: row.get('input_count'),
                outputCount: row.get('output_count'),
                coin: transaction.coin
            }
        })
        return res[0]
    }

}