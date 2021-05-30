import { AddressCluster } from '../models/address-cluster'
import { Resolver, FieldResolver, Root, InputType, Field, Args, ArgsType } from 'type-graphql'
import { Inject } from 'typedi'
import { types } from 'cassandra-driver'
import { PaginatedClusterTransactionResponse, ClusterTransactionCursor, ClusterTransaction } from '../models/cluster-transaction'
import { PaginatedAddressResponse, Address } from '../models/address'
import { LimitedCapacityClient } from '../limited-capacity-client'
import { AddressClusterDailyBalanceChange, AddressClusterDailyBalanceChangeCursor, PaginatedAddressClusterDailyBalanceChangeResponse } from '../models/address-cluster-daily-balance-change'
import { AddressClusterDetails } from '../models/address-cluster-details'
import { CoinResolver } from './coin-resolver'
import { PaginationArgs } from './pagination-args'

@InputType()
export class AddressCursor {

    @Field({ nullable: false })
    address: string

}

@ArgsType()
class ClusterTransactionsArgs extends PaginationArgs {

    @Field({ nullable: true })
    cursor: ClusterTransactionCursor

}

@ArgsType()
class DailyBalanceChangeArgs extends PaginationArgs {

    @Field({ nullable: true })
    cursor: AddressClusterDailyBalanceChangeCursor

}

@ArgsType()
class ClusterAddressesArgs extends PaginationArgs {
    @Field({ nullable: true })
    cursor: AddressCursor
}

function hashCode(s) {
    let h
    for (let i = 0; i < s.length; i++)
        h = Math.imul(31, h) + s.charCodeAt(i) | 0

    return h
}

@Resolver(of => AddressCluster)
export class AddressClusterResolver {

    constructor(@Inject('cassandra_client') private client: LimitedCapacityClient) {
    }

    static CLUSTER_DAILY_BALANCES_BIN_COUNT = 20

    @FieldResolver(returns => PaginatedClusterTransactionResponse, { nullable: false, complexity: ({ childComplexity, args }) => 100 + args.limit * childComplexity })
    async transactions(@Root() cluster: AddressCluster,
        @Args() { limit, cursor }: ClusterTransactionsArgs
    ): Promise<PaginatedClusterTransactionResponse> {
        let args: any[] = [cluster.clusterId]
        let query: string = 'SELECT timestamp, height, tx_n, balance_change FROM ' + cluster.coin.keyspace + '.cluster_transaction WHERE cluster_id=?'
        if (cursor) {
            query += ' AND (timestamp, height, tx_n) < (?, ?, ?)'
            args = args.concat([cursor.timestamp, cursor.height, cursor.txN])
        }
        query += ' LIMIT ?'
        args.push(limit + 1)
        const resultSet: types.ResultSet = await this.client.execute(
            query,
            args,
            { prepare: true, fetchSize: null }
        )
        const hasMore: boolean = resultSet.rows.length > limit
        if (hasMore) resultSet.rows.pop()
        const res: ClusterTransaction[] = resultSet.rows.map(row => {
            return <ClusterTransaction>{
                timestamp: row.get('timestamp'),
                height: row.get('height'),
                txN: row.get('tx_n'),
                balanceChange: row.get('balance_change'),
                coin: cluster.coin
            }
        })
        return {
            hasMore: hasMore,
            items: res,
        }
    }

    @FieldResolver(returns => PaginatedAddressResponse, { nullable: false, complexity: ({ childComplexity, args }) => 100 + args.limit * childComplexity })
    async addresses(@Root() cluster: AddressCluster,
        @Args() { limit, cursor }: ClusterAddressesArgs
    ): Promise<PaginatedAddressResponse> {
        let args: any[] = [cluster.clusterId]
        let query: string = 'SELECT address FROM ' + cluster.coin.keyspace + '.cluster_address WHERE cluster_id=?'
        if (cursor) {
            query += ' AND address > ?'
            args = args.concat([cursor.address])
        }
        query += ' LIMIT ?'
        args.push(limit + 1)
        const resultSet: types.ResultSet = await this.client.execute(
            query,
            args,
            { prepare: true, fetchSize: null }
        )
        const hasMore: boolean = resultSet.rows.length > limit
        if (hasMore) resultSet.rows.pop()
        const res: Address[] = resultSet.rows.map(row => new Address({ address: row.get('address'), coin: cluster.coin }))
        return {
            hasMore: hasMore,
            items: res,
        }
    }

    @FieldResolver(returns => PaginatedAddressClusterDailyBalanceChangeResponse, { nullable: false, complexity: ({ childComplexity, args }) => 100 + args.limit * childComplexity })
    async dailyBalanceChanges(@Root() cluster: AddressCluster,
        @Args() { limit, cursor }: DailyBalanceChangeArgs
    ): Promise<PaginatedAddressClusterDailyBalanceChangeResponse> {
        const bin = Math.abs(hashCode(cluster.clusterId)) % AddressClusterResolver.CLUSTER_DAILY_BALANCES_BIN_COUNT
        let args: any[] = [cluster.clusterId, bin]
        let query: string = 'SELECT date, balance_change FROM ' + cluster.coin.keyspace + '.cluster_daily_balance_change WHERE cluster_id=? AND bin = ?'
        if (cursor) {
            query += ' AND date > ?'
            args = args.concat([cursor.date])
        }
        args.push(limit + 1)
        query += ' ORDER BY date ASC LIMIT ?'
        const resultSet: types.ResultSet = await this.client.execute(
            query,
            args,
            { prepare: true, fetchSize: null }
        )
        const hasMore: boolean = resultSet.rows.length > limit
        if (hasMore) resultSet.rows.pop()
        const res: AddressClusterDailyBalanceChange[] = resultSet.rows.map(row => {
            return <AddressClusterDailyBalanceChange>{
                date: row.get('date'),
                balanceChange: row.get('balance_change')
            }
        })
        return {
            hasMore: hasMore,
            items: res,
        }
    }

    @FieldResolver(returns => AddressClusterDetails, { nullable: true, complexity: ({ childComplexity, args }) => 100 })
    async details(@Root() cluster: AddressCluster,
    ): Promise<AddressClusterDetails> {
        const bin = Math.abs(hashCode(cluster.clusterId)) % CoinResolver.CLUSTER_RICHLIST_BIN_COUNT
        const args: any[] = [cluster.clusterId, bin]
        const query: string = 'SELECT * FROM ' + cluster.coin.keyspace + '.cluster_details WHERE cluster_id=? AND bin = ?'
        const resultSet: types.ResultSet = await this.client.execute(
            query,
            args,
            { prepare: true, fetchSize: null }
        )
        const res: AddressClusterDetails[] = resultSet.rows.map(row => {
            return <AddressClusterDetails>{
                balance: row.get('balance')
            }
        })
        return res[0]
    }

}