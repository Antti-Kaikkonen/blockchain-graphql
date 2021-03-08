import { ObjectType, Field } from "type-graphql";
import { Coin } from "./coin";

@ObjectType()
export class AddressCluster {

    @Field({ nullable: false, complexity: 1 })
    clusterId: string;

    coin: Coin;
}