import { ObjectType, InputType, Field } from "type-graphql"
import { PaginatedResponse } from "./paginated-response"

@ObjectType()
export class OHLC {

    @Field({ nullable: false, complexity: 1 })
    open: number

    @Field({ nullable: false, complexity: 1 })
    high: number

    @Field({ nullable: false, complexity: 1 })
    low: number

    @Field({ nullable: false, complexity: 1 })
    close: number

    @Field({ nullable: false, complexity: 1 })
    timestamp: Date

}

@InputType()
export class OHLCCursor {

    @Field({ nullable: false })
    timestamp: Date

}

@ObjectType()
export class PaginatedOHLCResponse extends PaginatedResponse(OHLC) {
    @Field({ nullable: false, complexity: 0 })
    hasMore: boolean
}