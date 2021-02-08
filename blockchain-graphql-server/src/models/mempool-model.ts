import { ObjectType } from "type-graphql";
import { Coin } from "./coin";

@ObjectType("Mempool")
export class MempoolModel {

  readonly coin: Coin;

}