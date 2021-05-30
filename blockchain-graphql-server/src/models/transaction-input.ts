import { ObjectType, Field, InputType, Int } from 'type-graphql'
import { ScriptSig } from './scriptsig'
import { PaginatedResponse } from './paginated-response'
import { Coin } from './coin'

@ObjectType()
export class TransactionInput {

    constructor({
        sequence, txid, vout, coinbase, scriptSig, spendingTxid, spendingIndex, coin
    }: {
        sequence: number, txid: string, vout: number, coinbase: string, scriptSig: ScriptSig, spendingTxid: string, spendingIndex: number, coin: Coin
    }) {
        this.sequence = sequence
        this.txid = txid
        this.vout = vout
        this.coinbase = coinbase
        this.scriptSig = scriptSig
        this.spendingTxid = spendingTxid
        this.spendingIndex = spendingIndex
        this.coin = coin
    }

    @Field({ nullable: false, complexity: 1 })
    readonly sequence: number
    @Field({ nullable: true, complexity: 1 })
    readonly txid: string
    @Field(type => Int, { nullable: true, complexity: 1 })
    readonly vout: number
    @Field({ nullable: true, complexity: 1 })
    readonly coinbase: string

    @Field({ nullable: false, complexity: 1 })
    readonly scriptSig: ScriptSig

    @Field({ nullable: false, complexity: 1 })
    readonly spendingTxid: string

    @Field(type => Int, { nullable: false, complexity: 1 })
    readonly spendingIndex: number

    @Field({ nullable: false, complexity: 1 })
    readonly coin: Coin

}

@InputType()
export class TransactionInputCursor {

    @Field(type => Int, { nullable: false })
    spendingIndex: number

}

@ObjectType()
export class PaginatedTransactionInputResponse extends PaginatedResponse(TransactionInput) {
    @Field({ nullable: false, complexity: 0 })
    hasMore: boolean
}