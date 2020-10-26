import { ObjectType, Field } from "type-graphql";
import { PaginatedAddressTransactionResponse } from "./address-transaction";
import { PaginatedOHLCResponse } from "./ohlc";
import { PaginatedAddressBalanceResponse } from "./address-balance";
import { PaginatedAddressBalanceChangeResponse } from "./address-balance-change";
import { AddressCluster } from "./address-cluster";
import { PaginatedResponse } from "./paginated-response";

@ObjectType()
export class Address {

  constructor(address: string) {
    this.address = address;
  }

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
}