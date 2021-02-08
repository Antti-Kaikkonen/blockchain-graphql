import { ObjectType, Field, Int, InputType } from "type-graphql";
import { Coin } from "./coin";
import { PaginatedResponse } from "./paginated-response";

@ObjectType()
export class BlockHash {

  @Field({nullable: false, complexity: 1})
  hash: string;

  @Field(type => Int, {nullable: false, complexity: 1})
  height: number;

  readonly coin: Coin;

}

@InputType()
export class BlockHashCursor {

  @Field(type => Int, {nullable: false})
  height: number;

}

@ObjectType()
export class PaginatedAddressBlockHashtResponse extends PaginatedResponse(BlockHash) {
  @Field({nullable: false, complexity: 0})
  hasMore: boolean;
}