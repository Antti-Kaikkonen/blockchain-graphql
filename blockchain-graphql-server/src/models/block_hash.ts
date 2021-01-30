import { ObjectType, Field, Int } from "type-graphql";
import { Coin } from "./coin";

@ObjectType()
export class BlockHash {

  @Field({nullable: false, complexity: 1})
  hash: string;

  @Field(type => Int, {nullable: false, complexity: 1})
  height: number;

  readonly coin: Coin;

}