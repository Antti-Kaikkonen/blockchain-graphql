import { ObjectType, Field, InputType } from "type-graphql"
import { PaginatedResponse } from "./paginated-response"

@ObjectType()
export class AddressClusterDailyBalanceChange {

    @Field({ nullable: false, complexity: 1 })
    date: string

    @Field({ nullable: false, complexity: 1 })
    balanceChange: number

}

@InputType()
export class AddressClusterDailyBalanceChangeCursor {

    @Field({ nullable: false })
    date: string

}

@ObjectType()
export class PaginatedAddressClusterDailyBalanceChangeResponse extends PaginatedResponse(AddressClusterDailyBalanceChange) {
    @Field({ nullable: false, complexity: 0 })
    hasMore: boolean
}