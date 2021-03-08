import { ObjectType, Field } from "type-graphql";
import { Coin } from "./coin";

@ObjectType()
export class Date {

    readonly coin: Coin;

    @Field({ nullable: false, complexity: 1 })
    date: string;

}