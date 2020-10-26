import { ObjectType, Field } from "type-graphql";
import { PaginatedClusterTransactionResponse } from "./cluster-transaction";
import { PaginatedAddressResponse } from "./address";

@ObjectType()
export class AddressCluster {

    @Field({complexity: 1, nullable: false})
    clusterId: string;

    @Field(type => PaginatedClusterTransactionResponse, {nullable: false})
    clusterTransactions: PaginatedClusterTransactionResponse;

    @Field(type => PaginatedAddressResponse, {nullable: false})
    clusterAddresses: PaginatedAddressResponse;
}