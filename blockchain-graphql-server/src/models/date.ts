import { ObjectType, Field } from "type-graphql";
import { Coin } from "./coin";
import { PaginatedRichlistResponse } from "./richlist";
import { PaginatedAddressBalanceChangeResponse } from "./address-balance-change";

@ObjectType()
export class Date {

    readonly coin: Coin;

    @Field({ nullable: false, complexity: 1 })
    date: string;

}