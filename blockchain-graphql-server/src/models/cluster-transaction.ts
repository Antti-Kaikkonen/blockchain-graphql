import { ObjectType, Field, InputType, Int } from 'type-graphql'
import { Coin } from './coin'
import { PaginatedResponse } from './paginated-response'

@ObjectType()
export class ClusterTransaction {

    @Field({ nullable: false, complexity: 1 })
    timestamp: Date

    @Field(type => Int, { nullable: false, complexity: 1 })
    height: number

    @Field(type => Int, { nullable: false, complexity: 1 })
    txN: number

    @Field({ nullable: false, complexity: 1 })
    balanceChange: number

    readonly coin: Coin

}

@InputType()
export class ClusterTransactionCursor {

    @Field({ nullable: false })
    timestamp: Date

    @Field(type => Int, { nullable: false })
    height: number

    @Field(type => Int, { nullable: false })
    txN: number

}

@ObjectType()
export class PaginatedClusterTransactionResponse extends PaginatedResponse(ClusterTransaction) {
    @Field({ nullable: false, complexity: 0 })
    hasMore: boolean
}