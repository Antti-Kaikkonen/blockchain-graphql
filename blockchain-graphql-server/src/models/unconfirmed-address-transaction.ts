import { ObjectType, Field, InputType } from 'type-graphql'
import { Coin } from './coin'
import { PaginatedResponse } from './paginated-response'

@ObjectType()
export class UnconfirmedAddressTransaction {

    @Field({ nullable: false, complexity: 1 })
    timestamp: Date

    @Field({ nullable: false, complexity: 1 })
    txid: string

    @Field({ nullable: false, complexity: 1 })
    balanceChange: number

    coin: Coin

}

@InputType()
export class UnconfirmedAddressTransactionCursor {

    @Field({ nullable: false })
    timestamp: Date

    @Field({ nullable: false })
    txid: string

}

@ObjectType()
export class PaginatedUnconfirmedAddressTransactionResponse extends PaginatedResponse(UnconfirmedAddressTransaction) {
    @Field({ nullable: false, complexity: 0 })
    hasMore: boolean
}