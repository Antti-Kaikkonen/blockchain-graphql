import { ObjectType, Field } from "type-graphql";
import { AddressTransaction } from "./address-transaction";
import { OHLC } from "./ohlc";
import { Richlist, PaginatedRichlistResponse } from "./richlist";
import { AddressBalanceChange, PaginatedAddressBalanceChangeResponse } from "./address-balance-change";

@ObjectType()
export class Date {

  @Field({nullable: false, complexity: 1})
  date: string;

  @Field(type => PaginatedRichlistResponse, {nullable: false})
  richlist: PaginatedRichlistResponse;

  @Field(type => PaginatedAddressBalanceChangeResponse, {nullable: false})
  top_gainers: PaginatedAddressBalanceChangeResponse;

  @Field(type => PaginatedAddressBalanceChangeResponse, {nullable: false})
  top_losers: PaginatedAddressBalanceChangeResponse;

  /*@Field(type => PaginatedAddressBalanceChangeResponse, {nullable: true})
  address_balance_changes: PaginatedAddressBalanceChangeResponse;*/

}