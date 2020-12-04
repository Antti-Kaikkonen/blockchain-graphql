import { ObjectType, Field, InputType, Int } from "type-graphql";
import { PaginatedResponse } from "./paginated-response";

@ObjectType()
export class AddressBalance {

    @Field({nullable: false, complexity: 1})
    balance!: number;

    @Field({nullable: false, complexity: 1})
    timestamp!: Date;

}

@InputType()
export class AddressBalanceCursor {

    @Field({nullable: false})
    timestamp: Date;

}

@ObjectType()
export class PaginatedAddressBalanceResponse extends PaginatedResponse(AddressBalance) {
  @Field({complexity: 0})
  hasMore: boolean;
}