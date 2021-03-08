import { ObjectType, Field } from "type-graphql";

@ObjectType()
export class AddressClusterDetails {

    @Field({ nullable: false, complexity: 1 })
    balance: number;

}