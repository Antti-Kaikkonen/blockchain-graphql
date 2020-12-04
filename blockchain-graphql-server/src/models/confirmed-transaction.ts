import { ObjectType, Field, InputType, Int } from "type-graphql";
import { Transaction } from "./transaction";
import { BlockHash } from "./block_hash";
import { PaginatedResponse } from "./paginated-response";
import { Coin } from "./coin";

@ObjectType()
export class ConfirmedTransaction {

  @Field({nullable: true})
  blockHash: BlockHash;

  @Field(type => Int, {nullable: false, complexity: 1})
  height!: number;

  @Field(type => Int, {nullable: false, complexity: 1})
  tx_n!: number;

  @Field({nullable: false, complexity: 1})
  txid!: string;

  @Field({nullable: false})
  transaction: Transaction;

  coin: Coin;

}

@InputType()
export class ConfirmedTransactionCursor {

  @Field(type => Int, {nullable: false})
  tx_n: number;

}

@ObjectType()
export class PaginatedConfirmedTransactionResponse extends PaginatedResponse(ConfirmedTransaction) {
  @Field({complexity: 0})
  hasMore: boolean;
}