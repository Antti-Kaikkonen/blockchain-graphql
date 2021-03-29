import { ObjectType, Field, InputType, Int } from "type-graphql";
import { PaginatedResponse } from "./paginated-response";
import { Coin } from "./coin";

@ObjectType()
export class ConfirmedTransaction {

    @Field(type => Int, { nullable: false, complexity: 1 })
    height: number;

    @Field(type => Int, { nullable: false, complexity: 1 })
    txN: number;

    @Field({ nullable: false, complexity: 1 })
    txid: string;

    @Field({ nullable: false, complexity: 1 })
    readonly coin: Coin;

}

@InputType()
export class ConfirmedTransactionCursor {

    @Field(type => Int, { nullable: false })
    txN: number;

}

@ObjectType()
export class PaginatedConfirmedTransactionResponse extends PaginatedResponse(ConfirmedTransaction) {
    @Field({ nullable: false, complexity: 0 })
    hasMore: boolean;
}