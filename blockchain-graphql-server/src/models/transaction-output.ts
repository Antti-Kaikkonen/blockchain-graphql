import { ObjectType, Field, InputType, Int } from "type-graphql";
import { ScriptPubKey } from "./scriptpubkey";
import { PaginatedResponse } from "./paginated-response";
import { Coin } from "./coin";

@ObjectType()
export class TransactionOutput {

    constructor(
        { txid, n, value, scriptPubKey, spendingTxid, spendingIndex, coin }:
            {
                txid: string, n: number, value: number, scriptPubKey: ScriptPubKey, spendingTxid?: string, spendingIndex?: number, coin: Coin
            }) {
        this.txid = txid;
        this.n = n;
        this.value = value;
        this.scriptPubKey = scriptPubKey;
        this.spendingTxid = spendingTxid;
        this.spendingIndex = spendingIndex;
        this.coin = coin;
    }

    @Field({ nullable: false, complexity: 1 })
    readonly txid: string;
    @Field(type => Int, { nullable: false, complexity: 1 })
    readonly n: number;
    @Field({ nullable: false, complexity: 1 })
    readonly value: number;
    @Field({ nullable: false, complexity: 1 })
    readonly scriptPubKey: ScriptPubKey;
    @Field({ nullable: true, complexity: 1 })
    spendingTxid: string;
    @Field(type => Int, { nullable: true, complexity: 1 })
    spendingIndex: number;

    readonly coin: Coin;
}

@InputType()
export class TransactionOutputCursor {

    @Field(type => Int, { nullable: false })
    n: number;

}

@ObjectType()
export class PaginatedTransactionOutputResponse extends PaginatedResponse(TransactionOutput) {

    @Field({ nullable: false, complexity: 0 })
    hasMore: boolean;

}