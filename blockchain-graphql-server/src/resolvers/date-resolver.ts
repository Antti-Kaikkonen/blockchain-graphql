import { Resolver, Args, ArgsType, Field, FieldResolver, Root } from "type-graphql"
import { Date } from "../models/date"
import { types } from "cassandra-driver"
import { RichListCursor, RichList, PaginatedRichlistResponse } from "../models/richlist"
import { Address } from "../models/address"
import { Inject } from "typedi"
import { AddressBalanceChange, AddressBalanceChangeCursor, PaginatedAddressBalanceChangeResponse } from "../models/address-balance-change"
import { PaginationArgs } from "./pagination-args"
import { LimitedCapacityClient } from "../limited-capacity-client"
import { AddressCluster } from "../models/address-cluster"
import { PaginatedAddressClusterBalanceChangeResponse, AddressClusterBalanceChangeCursor, AddressClusterBalanceChange } from "../models/address-cluster-balance-change"

@ArgsType()
class RichlistArgs extends PaginationArgs {

    @Field({ nullable: true })
    cursor: RichListCursor

}

@ArgsType()
class AddressBalanceChangeArgs extends PaginationArgs {

    @Field({ nullable: true })
    cursor: AddressBalanceChangeCursor

}

@ArgsType()
class AddressClusterBalanceChangeArgs extends PaginationArgs {

    @Field({ nullable: true })
    cursor: AddressClusterBalanceChangeCursor

}

@Resolver(of => Date)
export class DateResolver {

    static BIN_COUNT = 20
    static BINS: number[] = Array.from(new Array(DateResolver.BIN_COUNT).keys())

    constructor(@Inject("cassandra_client") private client: LimitedCapacityClient) {
    }

    @FieldResolver(returns => PaginatedRichlistResponse, { nullable: false, complexity: ({ childComplexity, args }) => 100 + args.limit * childComplexity })
    async richList(
        @Root() date: Date,
        @Args() { cursor, limit }: RichlistArgs
    ): Promise<PaginatedRichlistResponse> {
        let args: any[] = [date.date, DateResolver.BINS]
        let query: string = "SELECT balance, balance_change, address FROM " + date.coin.keyspace + ".daily_richlist WHERE date=? AND bin IN ?"
        if (cursor) {
            query += " AND (balance, balance_change, address) < (?, ?, ?)"
            args = args.concat([Math.round(cursor.balance), Math.round(cursor.balanceChange), cursor.address])
        }
        args.push(limit + 1)
        query += " ORDER BY balance DESC, balance_change DESC, address DESC LIMIT ?"
        const resultSet: types.ResultSet = await this.client.execute(
            query,
            args,
            { prepare: true, fetchSize: null }
        )
        const hasMore: boolean = resultSet.rows.length > limit
        if (hasMore) resultSet.rows.pop()
        const res: RichList[] = resultSet.rows.map(row => {
            return <RichList>{
                address: new Address({ address: row.get("address"), coin: date.coin }),
                balance: row.get("balance"),
                balanceChange: row.get("balance_change")
            }
        })
        return {
            items: res,
            hasMore: hasMore
        }
    }

    @FieldResolver(returns => PaginatedAddressBalanceChangeResponse, { nullable: false, complexity: ({ childComplexity, args }) => 100 + args.limit * childComplexity })
    async topGainers(@Root() date: Date,
        @Args() { cursor, limit }: AddressBalanceChangeArgs
    ): Promise<PaginatedAddressBalanceChangeResponse> {
        const reverse = false
        let args: any[] = [date.date, DateResolver.BINS]
        let query: string = "SELECT address, balance_change FROM " + date.coin.keyspace + ".daily_top_gainers WHERE date=? AND bin IN ?"
        if (cursor) {
            query += " AND (balance_change, address) " + (reverse ? ">" : "<") + " (?, ?)"
            args = args.concat([cursor.balanceChange, cursor.address])
        }
        args.push(limit + 1)
        if (reverse) {
            query += " ORDER BY balance_change ASC, address ASC LIMIT ?"
        } else {
            query += " ORDER BY balance_change DESC, address DESC LIMIT ?"
        }
        const resultSet: types.ResultSet = await this.client.execute(
            query,
            args,
            { prepare: true, fetchSize: null }
        )
        const hasMore: boolean = resultSet.rows.length > limit
        if (hasMore) resultSet.rows.pop()
        const res: AddressBalanceChange[] = resultSet.rows.map(row => {
            return <AddressBalanceChange>{
                address: new Address({ address: row.get("address"), coin: date.coin }),
                balanceChange: row.get("balance_change")
            }
        })
        return {
            items: res,
            hasMore: hasMore
        }
    }

    @FieldResolver(returns => PaginatedAddressBalanceChangeResponse, { nullable: false, complexity: ({ childComplexity, args }) => args.limit * childComplexity })
    async topLosers(@Root() date: Date,
        @Args() { cursor, limit }: AddressBalanceChangeArgs
    ): Promise<PaginatedAddressBalanceChangeResponse> {
        const reverse = false
        let args: any[] = [date.date, DateResolver.BINS]
        let query: string = "SELECT address, balance_change FROM " + date.coin.keyspace + ".daily_top_losers WHERE date=? AND bin IN ?"
        if (cursor) {
            query += " AND (balance_change, address) " + (reverse ? "<" : ">") + " (?, ?)"
            args = args.concat([Math.round(cursor.balanceChange), cursor.address])
        }
        args.push(limit + 1)
        if (reverse) {
            query += " ORDER BY balance_change DESC, address DESC LIMIT ?"
        } else {
            query += " ORDER BY balance_change ASC, address ASC LIMIT ?"
        }
        const resultSet: types.ResultSet = await this.client.execute(
            query,
            args,
            { prepare: true, fetchSize: null }
        )
        const hasMore: boolean = resultSet.rows.length > limit
        if (hasMore) resultSet.rows.pop()
        const res: AddressBalanceChange[] = resultSet.rows.map(row => {
            return <AddressBalanceChange>{
                address: new Address({ address: row.get("address"), coin: date.coin }),
                balanceChange: row.get("balance_change")
            }
        })
        return {
            items: res,
            hasMore: hasMore
        }
    }

    @FieldResolver(returns => PaginatedAddressClusterBalanceChangeResponse, { nullable: false, complexity: ({ childComplexity, args }) => 100 + args.limit * childComplexity })
    async topClusterGainers(@Root() date: Date,
        @Args() { cursor, limit }: AddressClusterBalanceChangeArgs
    ): Promise<PaginatedAddressClusterBalanceChangeResponse> {
        let args: any[] = [date.date, DateResolver.BINS]
        let query: string = "SELECT balance_change, cluster_id FROM " + date.coin.keyspace + ".daily_top_cluster_gainers WHERE date=? AND bin IN ?"
        if (cursor) {
            query += " AND (balance_change, cluster_id) < " + " (?, ?)"
            args = args.concat([cursor.balanceChange, cursor.clusterId])
        }
        args.push(limit + 1)
        query += " ORDER BY balance_change DESC, cluster_id DESC LIMIT ?"
        const resultSet: types.ResultSet = await this.client.execute(
            query,
            args,
            { prepare: true, fetchSize: null }
        )
        const hasMore: boolean = resultSet.rows.length > limit
        if (hasMore) resultSet.rows.pop()
        const res: AddressClusterBalanceChange[] = resultSet.rows.map(row => {
            return <AddressClusterBalanceChange>{
                guestimatedWallet: <AddressCluster>{
                    clusterId: row.get("cluster_id"),
                    coin: date.coin
                },
                balanceChange: row.get("balance_change")
            }
        })
        return {
            items: res,
            hasMore: hasMore
        }
    }

    @FieldResolver(returns => PaginatedAddressClusterBalanceChangeResponse, { nullable: false, complexity: ({ childComplexity, args }) => 100 + args.limit * childComplexity })
    async topClusterLosers(@Root() date: Date,
        @Args() { cursor, limit }: AddressClusterBalanceChangeArgs
    ): Promise<PaginatedAddressClusterBalanceChangeResponse> {
        let args: any[] = [date.date, DateResolver.BINS]
        let query: string = "SELECT balance_change, cluster_id FROM " + date.coin.keyspace + ".daily_top_cluster_losers WHERE date=? AND bin IN ?"
        if (cursor) {
            query += " AND (balance_change, cluster_id) > " + " (?, ?)"
            args = args.concat([cursor.balanceChange, cursor.clusterId])
        }
        args.push(limit + 1)
        query += " ORDER BY balance_change ASC, cluster_id ASC LIMIT ?"
        const resultSet: types.ResultSet = await this.client.execute(
            query,
            args,
            { prepare: true, fetchSize: null }
        )
        const hasMore: boolean = resultSet.rows.length > limit
        if (hasMore) resultSet.rows.pop()
        const res: AddressClusterBalanceChange[] = resultSet.rows.map(row => {
            return <AddressClusterBalanceChange>{
                guestimatedWallet: <AddressCluster>{
                    clusterId: row.get("cluster_id"),
                    coin: date.coin
                },
                balanceChange: row.get("balance_change")
            }
        })
        return {
            items: res,
            hasMore: hasMore
        }
    }

}