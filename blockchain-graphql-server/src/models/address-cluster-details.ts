import { ObjectType, Field } from "type-graphql";

@ObjectType()
export class AddressClusterDetails {

    @Field({complexity: 1, nullable: false})
    balance: number;

}