import { ObjectType, Field, InputType, Int } from "type-graphql";
import { ConfirmedTransaction } from "./confirmed-transaction";
import { PaginatedResponse } from "./paginated-response";

@ObjectType()
export class ClusterTransaction {

  @Field({complexity: 1})
  timestamp!: Date;
  
  @Field(type => Int, {complexity: 1})
  height!: number;

  @Field(type => Int, {complexity: 1})
  tx_n!: number;

  @Field({complexity: 1})
  balance_change!: number;

  @Field({nullable: false})
  confirmedTransaction: ConfirmedTransaction;

}

@InputType()
export class ClusterTransactionCursor {

  @Field()
  timestamp: Date;

  @Field(type=>Int)
  height: number;

  @Field(type=>Int)
  tx_n: number;

}

@ObjectType()
export class PaginatedClusterTransactionResponse extends PaginatedResponse(ClusterTransaction) {
}