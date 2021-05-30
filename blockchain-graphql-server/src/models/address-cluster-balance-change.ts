import { ObjectType, Field, InputType } from "type-graphql"
import { AddressCluster } from "./address-cluster"
import { PaginatedResponse } from "./paginated-response"

@ObjectType()
export class AddressClusterBalanceChange {

    @Field(type => AddressCluster, { nullable: false })
    guestimatedWallet: AddressCluster;

    @Field({ nullable: false, complexity: 1 })
    balanceChange: number;

}

@InputType()
export class AddressClusterBalanceChangeCursor {

    @Field({ nullable: false })
    balanceChange: number;

    @Field({ nullable: false })
    clusterId: string;

}

@ObjectType()
export class PaginatedAddressClusterBalanceChangeResponse extends PaginatedResponse(AddressClusterBalanceChange) {
    @Field({ nullable: false, complexity: 0 })
    hasMore: boolean;
}