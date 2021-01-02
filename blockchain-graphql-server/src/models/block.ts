import { ObjectType, Field, Int } from "type-graphql";
import { AddressTransaction, PaginatedAddressTransactionResponse } from "./address-transaction";
import { OHLC, PaginatedOHLCResponse } from "./ohlc";
import { ConfirmedTransaction, PaginatedConfirmedTransactionResponse } from "./confirmed-transaction";
import { Coin } from "./coin";

@ObjectType()
export class Block {

  @Field({nullable: false, complexity: 1})
  hash!: string;

  @Field(type => Int, {nullable: false, complexity: 1})
  height!: number;

  @Field(type => Int, {nullable: false, complexity: 1})
  size!: number;

  @Field(type => Int, {nullable: true, complexity: 1})
  version: number;

  @Field({nullable: false, complexity: 1})
  versionhex!: string;

  @Field({nullable: false, complexity: 1})
  merkleroot!: string;

  @Field({nullable: false, complexity: 1})
  time!: Date;

  @Field(type => Int, {nullable: false, complexity: 1})
  mediantime!: number;

  @Field(type => Int, {nullable: false, complexity: 1})
  nonce!: number;

  @Field({nullable: false, complexity: 1})
  bits!: string;

  @Field({nullable: false, complexity: 1})
  difficulty!: number;

  @Field({nullable: false, complexity: 1})
  chainwork!: string;

  @Field({nullable: false, complexity: 1})
  previousblockhash!: string;

  @Field(type => Int, {nullable: false, complexity: 1})
  tx_count: number;

  @Field(type => PaginatedConfirmedTransactionResponse, {nullable: false})
  confirmedTransactions: PaginatedConfirmedTransactionResponse;

  coin: Coin;

}