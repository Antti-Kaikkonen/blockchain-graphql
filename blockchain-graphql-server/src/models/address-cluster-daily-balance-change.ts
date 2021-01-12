import { ObjectType, Field, InputType } from "type-graphql";
import { Address } from "./address";
import { AddressCluster } from "./address-cluster";
import { PaginatedResponse } from "./paginated-response";

@ObjectType()
export class AddressClusterDailyBalanceChange {

    @Field()
    date: string;

    @Field()
    balance_change!: number;
    
}

@InputType()
export class AddressClusterDailyBalanceChangeCursor {

    @Field({nullable: false})
    date: string;

}

@ObjectType()
export class PaginatedAddressClusterDailyBalanceChangeResponse extends PaginatedResponse(AddressClusterDailyBalanceChange) {
    @Field({complexity: 0})
    hasMore: boolean;
}