import { ObjectType, Field } from "type-graphql";
import { PaginatedClusterTransactionResponse } from "./cluster-transaction";
import { PaginatedAddressResponse } from "./address";
import { Coin } from "./coin";

@ObjectType()
export class AddressCluster {

    @Field({complexity: 1, nullable: false})
    clusterId: string;

    coin: Coin;
}