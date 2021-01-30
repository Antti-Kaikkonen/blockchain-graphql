import { ObjectType, Field, InputType, Int } from "type-graphql";
import { Coin } from "./coin";
import { ConfirmedTransaction } from "./confirmed-transaction";
import { PaginatedResponse } from "./paginated-response";

@ObjectType()
export class ClusterTransaction {

  @Field({complexity: 1})
  timestamp!: Date;
  
  @Field(type => Int, {complexity: 1})
  height!: number;

  @Field(type => Int, {complexity: 1})
  txN!: number;

  @Field({complexity: 1})
  balanceChange!: number;

  @Field({nullable: false})
  confirmedTransaction: ConfirmedTransaction;

  coin: Coin;

}

@InputType()
export class ClusterTransactionCursor {

  @Field()
  timestamp: Date;

  @Field(type=>Int)
  height: number;

  @Field(type=>Int)
  txN: number;

}

@ObjectType()
export class PaginatedClusterTransactionResponse extends PaginatedResponse(ClusterTransaction) {
  @Field({complexity: 0})
  hasMore: boolean;
}