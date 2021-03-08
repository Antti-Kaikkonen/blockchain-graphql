import { ObjectType, Field, InputType } from "type-graphql";
import { Address } from "./address";
import { PaginatedResponse } from "./paginated-response";

@ObjectType()
export class RichList {

    @Field({ nullable: false, complexity: 1 })
    address: Address;

    @Field({ nullable: false, complexity: 1 })
    balance: number;

    @Field({ nullable: false, complexity: 1 })
    balanceChange: number;

}

@InputType()
export class RichListCursor {

    @Field({ nullable: true })
    balanceChange: number;

    @Field({ nullable: false })
    address: string;

    @Field({ nullable: false })
    balance: number;

}

@ObjectType()
export class PaginatedRichlistResponse extends PaginatedResponse(RichList) {
    @Field({ nullable: false, complexity: 0 })
    hasMore: boolean;
}