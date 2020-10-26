import { ObjectType, Field, Int } from "type-graphql";
import { AddressTransaction, PaginatedAddressTransactionResponse } from "./address-transaction";
import { OHLC, PaginatedOHLCResponse } from "./ohlc";
import { ConfirmedTransaction } from "./confirmed-transaction";
import { Block } from "./block";

@ObjectType()
export class BlockHash {

  @Field({nullable: false, complexity: 1})
  hash!: string;

  @Field(type => Int, {nullable: false, complexity: 1})
  height!: number;

  @Field(type => Block, {nullable: false})
  block: Block;

}