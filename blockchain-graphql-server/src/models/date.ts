import { ObjectType, Field } from "type-graphql"
import { Coin } from "./coin"

@ObjectType()
export class Date {

    @Field({ nullable: false, complexity: 1 })
    readonly coin: Coin

    @Field({ nullable: false, complexity: 1 })
    date: string

}