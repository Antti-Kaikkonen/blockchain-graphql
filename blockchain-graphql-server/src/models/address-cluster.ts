import { ObjectType, Field } from "type-graphql";
import { PaginatedClusterTransactionResponse } from "./cluster-transaction";
import { PaginatedAddressResponse } from "./address";
import { Coin } from "./coin";

@ObjectType()
export class AddressCluster {

    @Field({ nullable: false, complexity: 1 })
    clusterId: string;

    coin: Coin;
}