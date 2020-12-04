import { ObjectType, Field, InputType, Int } from "type-graphql";
import { ScriptPubKey } from "./scriptpubkey";
import { TransactionInput } from "./transaction-input";
import { Transaction } from "./transaction";
import { PaginatedResponse } from "./paginated-response";
import { Coin } from "./coin";

@ObjectType()
export class TransactionOutput {
    @Field({nullable: false, complexity: 1})
    txid: string;
    @Field(type => Int, {nullable: false, complexity: 1})
    n: number;
    @Field({nullable: false, complexity: 1})
    value: number;
    @Field({nullable: false, complexity: 1})
    scriptpubkey: ScriptPubKey;
    @Field({nullable: true, complexity: 1})
    spending_txid: string;
    @Field(type => Int, {nullable: true, complexity: 1})
    spending_index: number;

    @Field(type => TransactionInput, {nullable: true, complexity: 1})
    spendingInput: TransactionInput

    @Field(type => Transaction, {nullable: false, complexity: 1})
    transaction: Transaction;

    coin: Coin;
}

@InputType()
export class TransactionOutputCursor {

    @Field({nullable: false})
    n: number;

}

@ObjectType()
export class PaginatedTransactionOutputResponse extends PaginatedResponse(TransactionOutput) {
    
    @Field({complexity: 0})
    hasMore: boolean;

}