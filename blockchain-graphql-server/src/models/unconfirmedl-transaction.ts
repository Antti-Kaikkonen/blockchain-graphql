import { Field, InputType, ObjectType } from 'type-graphql'
import { Coin } from './coin'
import { PaginatedResponse } from './paginated-response'

@ObjectType()
export class UnconfirmedTransaction {
    @Field({ nullable: false, complexity: 1 })
    txid: string

    @Field({ nullable: false, complexity: 1 })
    timestamp: Date

    @Field({ nullable: false, complexity: 1 })
    coin: Coin

}

@InputType()
export class UnconfirmedTransactionCursor {

    @Field({ nullable: false })
    timestamp: Date

    @Field({ nullable: false })
    txid: string

}

@ObjectType()
export class PaginatedUnconfirmedTransactionResponse extends PaginatedResponse(UnconfirmedTransaction) {
    @Field({ nullable: false, complexity: 0 })
    hasMore: boolean
}