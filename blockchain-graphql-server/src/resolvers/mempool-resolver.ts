import { Args, ArgsType, Field, FieldResolver, Float, Int, Resolver, Root } from 'type-graphql'
import { MempoolModel } from '../models/mempool-model'
import { UnconfirmedTransaction, UnconfirmedTransactionCursor, PaginatedUnconfirmedTransactionResponse } from '../models/unconfirmedl-transaction'
import { PaginationArgs } from './pagination-args'

@ArgsType()
class UnconfirmedTransactionsArgs extends PaginationArgs {

    @Field({ nullable: true })
    cursor: UnconfirmedTransactionCursor

}

@Resolver(of => MempoolModel)
export class MempoolResolver {
    @FieldResolver(returns => Float, { nullable: false, complexity: ({ childComplexity, args }) => 1 })
    async totalFees(@Root() mempoolModel: MempoolModel): Promise<number> {
        return mempoolModel.coin.mempool.unconfirmedMempool.totalFeesSat / 1e8
    }

    @FieldResolver(returns => Int, { nullable: false, complexity: ({ childComplexity, args }) => 1 })
    async txCount(@Root() mempoolModel: MempoolModel): Promise<number> {
        return mempoolModel.coin.mempool.unconfirmedMempool.txs.size
    }

    @FieldResolver(returns => PaginatedUnconfirmedTransactionResponse, { nullable: false, complexity: ({ childComplexity, args }) => args.limit * childComplexity })
    async transactions(@Root() mempoolModel: MempoolModel, @Args() { cursor, limit }: UnconfirmedTransactionsArgs): Promise<PaginatedUnconfirmedTransactionResponse> {
        let it
        if (cursor) {
            it = mempoolModel.coin.mempool.unconfirmedMempool.txids.upperBound({ timestamp: cursor.timestamp.getTime(), txid: cursor.txid })
        } else {
            it = mempoolModel.coin.mempool.unconfirmedMempool.txids.iterator()
            it.next()
        }
        let item: { txid: string, timestamp: number } = it.data()
        const res: UnconfirmedTransaction[] = []
        let hasMore = false
        while (item !== null) {
            if (res.length === limit) {
                hasMore = true
                break
            }
            res.push(<UnconfirmedTransaction>{ coin: mempoolModel.coin, timestamp: new Date(item.timestamp), txid: item.txid })
            item = it.next()
        }
        return { items: res, hasMore: hasMore }
    }
}