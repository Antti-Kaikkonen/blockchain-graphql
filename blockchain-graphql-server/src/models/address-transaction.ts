import { ObjectType, Field, InputType, Int } from "type-graphql";
import { type } from "os";
import { PaginatedResponse } from "./paginated-response";
import { ConfirmedTransaction } from "./confirmed-transaction";
import { Coin } from "./coin";

@ObjectType()
export class AddressTransaction {
  
  @Field({complexity: 1})
  timestamp!: Date;

  @Field({nullable: false})
  confirmedTransaction: ConfirmedTransaction;
  
  @Field(type => Int, {complexity: 1})
  height!: number;

  @Field(type => Int, {complexity: 1})
  txN!: number;

  @Field({complexity: 1})
  balanceChange!: number;

  @Field({nullable: false, complexity: 1})
  balanceAfterBlock: number;

  coin: Coin;

}

@InputType()
export class AddressTransactionCursor {

  @Field()
  timestamp: Date;

  @Field(type=>Int)
  height: number;

  @Field(type=>Int)
  txN: number;

}

@ObjectType()
export class PaginatedAddressTransactionResponse extends PaginatedResponse(AddressTransaction) {
  @Field({complexity: 0})
  hasMore: boolean;
}