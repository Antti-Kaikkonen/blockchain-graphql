import { ObjectType, Field, InputType, Int } from "type-graphql";
import { ScriptPubKey } from "./scriptpubkey";
import { ScriptSig } from "./scriptsig";
import { TransactionOutput } from "./transaction-output";
import { Transaction } from "./transaction";
import { PaginatedResponse } from "./paginated-response";

@ObjectType()
export class TransactionInput {
    @Field({nullable: false, complexity: 1})
    sequence: number;
    @Field({nullable: true, complexity: 1})
    txid: string;
    @Field(type => Int, {nullable: true, complexity: 1})
    vout: number;
    @Field({nullable: true, complexity: 1})
    coinbase: string;

    @Field({nullable: false, complexity: 1})
    scriptsig: ScriptSig;

    @Field({nullable: false, complexity: 1})
    spending_txid: string;

    @Field(type => Int, {nullable: false, complexity: 1})
    spending_index: number;

    @Field(type => TransactionOutput, {nullable: true, complexity: 1})
    spentOutput: TransactionOutput;

    @Field(type => Transaction, {nullable: false, complexity: 1})
    transaction: Transaction;

}

@InputType()
export class TransactionInputCursor {

    @Field(type => Int, {nullable: false})
    spending_index: number;

}

@ObjectType()
export class PaginatedTransactionInputResponse extends PaginatedResponse(TransactionInput) {
}