import { ObjectType, Field, Int } from "type-graphql";
import { PaginatedTransactionOutputResponse } from "./transaction-output";
import { PaginatedTransactionInputResponse } from "./transaction-input";
import { BlockHash } from "./block_hash";
import { Coin } from "./coin";

@ObjectType()
export class Transaction {

    @Field({ nullable: false, complexity: 1 })
    txid: string;

    @Field(type => Int, { nullable: true, complexity: 1 })
    height: number;

    @Field(type => Int, { nullable: true, complexity: 1 })
    txN: number;

    @Field(type => Int, { nullable: false, complexity: 1 })
    size: number;

    @Field(type => Int, { nullable: false, complexity: 1 })
    version: number;

    @Field(type => Int, { nullable: false, complexity: 1 })
    lockTime: number;

    @Field({ nullable: false, complexity: 1 })
    fee: number;

    readonly coin: Coin;

}


