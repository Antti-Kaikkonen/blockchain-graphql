import { ObjectType, Field, InputType } from "type-graphql";
import { Address } from "./address";
import { PaginatedResponse } from "./paginated-response";

@ObjectType()
export class AddressBalanceChange {

    @Field(type => Address, {complexity: 1})
    address: Address;

    @Field({complexity: 1})
    balanceChange!: number;
    
}

@InputType()
export class AddressBalanceChangeCursor {

    @Field({nullable: true, defaultValue: ""})
    address: string;

    @Field({nullable: false})
    balanceChange: number;

}

@ObjectType()
export class PaginatedAddressBalanceChangeResponse extends PaginatedResponse(AddressBalanceChange) {
    @Field({complexity: 0})
    hasMore: boolean;
}