import { ObjectType, Field, InputType } from "type-graphql";
import { Address } from "./address";
import { PaginatedResponse } from "./paginated-response";

@ObjectType()
export class AddressBalanceChange {

    @Field(type => Address, { nullable: false, complexity: 1 })
    address: Address;

    @Field({ nullable: false, complexity: 1 })
    balanceChange: number;

}

@InputType()
export class AddressBalanceChangeCursor {

    @Field({ nullable: false })
    balanceChange: number;

    @Field({ nullable: false })
    address: string;

}

@ObjectType()
export class PaginatedAddressBalanceChangeResponse extends PaginatedResponse(AddressBalanceChange) {
    @Field({ nullable: false, complexity: 0 })
    hasMore: boolean;
}