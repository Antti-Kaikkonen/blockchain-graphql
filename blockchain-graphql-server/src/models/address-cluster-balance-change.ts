import { ObjectType, Field, InputType } from "type-graphql";
import { Address } from "./address";
import { AddressCluster } from "./address-cluster";
import { PaginatedResponse } from "./paginated-response";

@ObjectType()
export class AddressClusterBalanceChange {

    @Field(type => AddressCluster, {nullable: false})
    guestimatedWallet: AddressCluster;

    @Field({complexity: 1})
    balanceChange!: number;
    
}

@InputType()
export class AddressClusterBalanceChangeCursor {

    @Field({nullable: true, defaultValue: ""})
    clusterId: string;

    @Field({nullable: false})
    balanceChange: number;

}

@ObjectType()
export class PaginatedAddressClusterBalanceChangeResponse extends PaginatedResponse(AddressClusterBalanceChange) {
    @Field({complexity: 0})
    hasMore: boolean;
}