import { Resolver, FieldResolver, Root, Field, Int, Args, ArgsType, Float } from 'type-graphql'
import { types } from 'cassandra-driver'
import { Inject, Service } from 'typedi'
import { Address } from '../models/address'
import { AddressTransaction, AddressTransactionCursor, PaginatedAddressTransactionResponse } from '../models/address-transaction'
import { OHLCCursor, OHLC, PaginatedOHLCResponse } from '../models/ohlc'
import { AddressBalanceCursor, PaginatedAddressBalanceResponse, AddressBalance } from '../models/address-balance'
import { AddressCluster } from '../models/address-cluster'
import { LimitedCapacityClient } from '../limited-capacity-client'
import { PaginationArgs } from './pagination-args'
import { PaginatedUnconfirmedAddressTransactionResponse, UnconfirmedAddressTransaction, UnconfirmedAddressTransactionCursor } from '../models/unconfirmed-address-transaction'

@ArgsType()
class OHLC_Args extends PaginationArgs {

    @Field({ nullable: true })
    cursor: OHLCCursor

    @Field(type => Int, { nullable: true })
    interval: number = 1000 * 60 * 60 * 24

}

@ArgsType()
class AddressTransactionsArgs extends PaginationArgs {

    @Field({ nullable: true })
    cursor: AddressTransactionCursor

}

@ArgsType()
class AddressBalancesArgs extends PaginationArgs {

    @Field({ nullable: true })
    cursor: AddressBalanceCursor

}

@ArgsType()
class UnconfirmedTransactionsArgs extends PaginationArgs {

    @Field({ nullable: true })
    cursor: UnconfirmedAddressTransactionCursor

}


@Service()
@Resolver(of => Address)
export class AddressResolver {

    constructor(
        @Inject('cassandra_client') private client: LimitedCapacityClient,
    ) { }


    @FieldResolver(returns => AddressCluster, { nullable: false, complexity: ({ childComplexity, args }) => 100 + childComplexity })
    async guestimatedWallet(@Root() address: Address): Promise<AddressCluster> {
        const query: string = 'SELECT parent FROM ' + address.coin.keyspace + '.union_find WHERE address=?'
        let currentAddress = address.address
        do {
            const resultSet: types.ResultSet = await this.client.execute(
                query,
                [currentAddress],
                { prepare: true }
            )
            if (resultSet.rows.length === 1) {
                currentAddress = resultSet.rows[0].get('parent')
            } else {
                const res: AddressCluster = new AddressCluster()
                res.clusterId = currentAddress
                res.coin = address.coin
                return res
            }
        } while (true)
    }


    @FieldResolver(returns => PaginatedOHLCResponse, { nullable: false, complexity: ({ childComplexity, args }) => 100 + args.limit * childComplexity })
    async ohlc(@Root() address: Address,
        @Args() { limit, cursor, interval }: OHLC_Args
    ): Promise<PaginatedOHLCResponse> {
        let args: unknown[] = [address.address, interval]
        let query: string = 'SELECT timestamp, open, high, low, close FROM ' + address.coin.keyspace + '.ohlc WHERE address=? AND interval=?'
        if (cursor) {
            query += ' AND timestamp > ?'
            args = args.concat([cursor.timestamp])
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
        const res: OHLC[] = resultSet.rows.map(row => {
            return <OHLC>{
                timestamp: row.get('timestamp'),
                open: row.get('open'),
                high: row.get('high'),
                low: row.get('low'),
                close: row.get('close')
            }
        })
        return {
            hasMore: hasMore,
            items: res,
        }
    }

    @FieldResolver(returns => PaginatedUnconfirmedAddressTransactionResponse, { nullable: false, complexity: ({ childComplexity, args }) => args.limit * childComplexity })
    async unconfirmedTransactions(@Root() address: Address, @Args() { cursor, limit }: UnconfirmedTransactionsArgs): Promise<PaginatedUnconfirmedAddressTransactionResponse> {
        let it
        const addressMempool = address.coin.mempool.unconfirmedMempool.addressMempools.get(address.address)
        if (addressMempool === undefined) {
            return { items: [], hasMore: false }
        }
        if (cursor) {
            it = addressMempool.transactions.upperBound({ timestamp: cursor.timestamp.getTime(), txid: cursor.txid, balanceChange: null })
        } else {
            it = addressMempool.transactions.iterator()
            it.next()
        }
        if (it === undefined) {
            return { items: [], hasMore: false }
        }
        let item: { txid: string, timestamp: number, balanceChange: number } = it.data()
        const res: UnconfirmedAddressTransaction[] = []
        let hasMore = false
        while (item !== null) {
            if (res.length === limit) {
                hasMore = true
                break
            }
            res.push(<UnconfirmedAddressTransaction>{ coin: address.coin, timestamp: new Date(item.timestamp), txid: item.txid, balanceChange: item.balanceChange })
            item = it.next()
        }
        return { items: res, hasMore: hasMore }
    }

    @FieldResolver(returns => Int, { nullable: false, complexity: ({ childComplexity, args }) => 1 })
    async unconfirmedTxCount(@Root() address: Address): Promise<number> {
        const addressUnconfirmedMempool = address.coin.mempool.unconfirmedMempool.addressMempools.get(address.address)
        if (addressUnconfirmedMempool === undefined) {
            return 0
        } else {
            return addressUnconfirmedMempool.transactions.size
        }
    }

    @FieldResolver(returns => Float, { nullable: false, complexity: ({ childComplexity, args }) => 1 })
    async unconfirmedBalanceChange(@Root() address: Address): Promise<number> {
        const addressUnconfirmedMempool = address.coin.mempool.unconfirmedMempool.addressMempools.get(address.address)
        if (addressUnconfirmedMempool === undefined) {
            return 0
        } else {
            return addressUnconfirmedMempool.balanceChangeSat / 1e8
        }
    }


    @FieldResolver(returns => PaginatedAddressTransactionResponse, { nullable: false, complexity: ({ childComplexity, args }) => 100 + args.limit * childComplexity })
    async confirmedTransactions(@Root() address: Address,
        @Args() { limit, cursor }: AddressTransactionsArgs
    ): Promise<PaginatedAddressTransactionResponse> {
        const originalLimit: number = limit
        let res: AddressTransaction[] = address.coin.mempool?.addressTransactions.get(address.address)
        if (res !== undefined) {
            if (cursor) {
                const lastIndex = res.findIndex((e) => {
                    if (e.timestamp.getTime() === cursor.timestamp.getTime()) {
                        if (e.height === cursor.height) {
                            return e.txN >= cursor.txN
                        }
                        return e.height > cursor.height
                    }
                    return e.timestamp.getTime() > cursor.timestamp.getTime()
                })//TODO: use binary search instead
                if (lastIndex !== -1) {
                    res = res.slice(0, lastIndex)
                    if (res.length > limit + 1) res = res.slice(res.length - (limit + 1))
                }
            }
            res.reverse()
            if (res.length > 0) {
                cursor = {
                    timestamp: res[res.length - 1].timestamp,
                    height: res[res.length - 1].height,
                    txN: res[res.length - 1].txN
                }
                limit = limit - res.length
            }
        } else {
            res = []
        }
        if (limit + 1 > 0) {

            let args: unknown[] = [address.address]
            let query: string = 'SELECT timestamp, height, tx_n, balance_change FROM ' + address.coin.keyspace + '.address_transaction WHERE address=?'
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
            const res2: AddressTransaction[] = resultSet.rows.map(row => {
                return <AddressTransaction>{
                    timestamp: row.get('timestamp'),
                    height: row.get('height'),
                    txN: row.get('tx_n'),
                    balanceChange: row.get('balance_change'),
                    coin: address.coin
                }
            })

            if (res2.length > 0) {
                const start = res2[res2.length - 1].timestamp
                const end = res2[0].timestamp
                const query2: string = 'SELECT timestamp, balance FROM ' + address.coin.keyspace + '.address_balance WHERE address=? AND timestamp >= ? AND timestamp <= ?'
                const args2: unknown[] = [address.address, start, end]
                const resultSet2: types.ResultSet = await this.client.execute(
                    query2,
                    args2,
                    { prepare: true }
                )
                const time2Balance: Map<number, number> = new Map()
                resultSet2.rows.forEach(row => time2Balance.set(row.get('timestamp').getTime(), row.get('balance')))
                res2.forEach(r => r.balanceAfterBlock = time2Balance.get(r.timestamp.getTime()))
                res = res.concat(res2)
            }

        }
        if (res.length > originalLimit) {
            return {
                hasMore: true,
                items: res.slice(0, originalLimit),
            }
        } else {
            return {
                hasMore: false,
                items: res,
            }
        }
    }

    @FieldResolver(returns => PaginatedAddressBalanceResponse, { nullable: false, complexity: ({ childComplexity, args }) => 100 + args.limit * childComplexity })
    async balances(@Root() address: Address,
        @Args() { limit, cursor }: AddressBalancesArgs
    ): Promise<PaginatedAddressBalanceResponse> {
        const originalLimit: number = limit
        let res: AddressBalance[] = address.coin.mempool?.addressBalances.get(address.address)
        if (res !== undefined) {
            if (cursor) {
                const lastIndex = res.findIndex((e) => e.timestamp >= cursor.timestamp)//TODO: use binary search instead
                if (lastIndex !== -1) {
                    res = res.slice(0, lastIndex)
                }
            }
            if (res.length > limit + 1) res = res.slice(res.length - (limit + 1))
            res.reverse()
            if (res.length > 0) {
                cursor = { timestamp: res[res.length - 1].timestamp }
                limit = limit - res.length
            }
        } else {
            res = []
        }
        if (limit + 1 > 0) {
            let args: unknown[] = [address.address]
            let query: string = 'SELECT timestamp, balance FROM ' + address.coin.keyspace + '.address_balance WHERE address=?'
            if (cursor) {
                query += ' AND timestamp < ?'
                args = args.concat([cursor.timestamp])
            }
            query += ' LIMIT ?'
            args.push(limit + 1)
            const resultSet: types.ResultSet = await this.client.execute(
                query,
                args,
                { prepare: true, fetchSize: null }
            )
            res = res.concat(resultSet.rows.map(row => {
                return <AddressBalance>{
                    timestamp: row.get('timestamp'),
                    balance: row.get('balance')
                }
            }))
        }
        if (res.length > originalLimit) {
            return {
                hasMore: true,
                items: res.slice(0, originalLimit),
            }
        } else {
            return {
                hasMore: false,
                items: res,
            }
        }
    }


}