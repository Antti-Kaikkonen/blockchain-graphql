import { types } from 'cassandra-driver'
import { FieldResolver, Resolver, Root } from 'type-graphql'
import { Inject, Service } from 'typedi'
import { LimitedCapacityClient } from '../limited-capacity-client'
import { MempoolTx } from '../mempool/mempool'
import { Transaction } from '../models/transaction'
import { UnconfirmedAddressTransaction } from '../models/unconfirmed-address-transaction'

@Service()
@Resolver(of => UnconfirmedAddressTransaction)
export class UnconfirmedAddressTransactionResolver {


    constructor(@Inject('cassandra_client') private client: LimitedCapacityClient) {
    }

    @FieldResolver(returns => Transaction, { nullable: false, complexity: ({ childComplexity, args }) => 100 + childComplexity })
    async transaction(@Root() rootTx: UnconfirmedAddressTransaction,
    ): Promise<Transaction> {
        const mempoolTransaction: MempoolTx = rootTx.coin.mempool?.txById.get(rootTx.txid)
        if (mempoolTransaction !== undefined) {
            return mempoolTransaction.toGraphQL(rootTx.coin)
        }
        const args: unknown[] = [rootTx.txid]
        const query: string = 'SELECT * FROM ' + rootTx.coin.keyspace + '.transaction WHERE txid=?'
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
                coin: rootTx.coin
            }
        })
        return res[0]
    }

}