import { ObjectType, Field } from "type-graphql";
import { PaginatedAddressTransactionResponse } from "./address-transaction";
import { PaginatedOHLCResponse } from "./ohlc";
import { PaginatedAddressBalanceResponse } from "./address-balance";
import { AddressCluster } from "./address-cluster";
import { PaginatedResponse } from "./paginated-response";
import { Coin } from "./coin";

@ObjectType()
export class Address {

  constructor(address: string, coin: Coin) {
    this.address = address;
    this.coin = coin;
  }

  coin: Coin;

  @Field({nullable: false, complexity: 1})
  address!: string;

  @Field(type => PaginatedAddressTransactionResponse, {nullable: false})
  addressTransactions: PaginatedAddressTransactionResponse;

  @Field(type => PaginatedOHLCResponse, {nullable: false})
  ohlc: PaginatedOHLCResponse;

  @Field(type => PaginatedAddressBalanceResponse, {nullable: false})
  addressBalances: PaginatedAddressBalanceResponse;

  @Field(type => AddressCluster, {nullable: true})
  guestimatedWallet: AddressCluster;

}

@ObjectType()
export class PaginatedAddressResponse extends PaginatedResponse(Address) {
  @Field({complexity: 0})
  hasMore: boolean;
}