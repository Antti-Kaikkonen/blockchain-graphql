import { Resolver, FieldResolver, Root, Field, ArgsType, Args } from "type-graphql"
import { types } from "cassandra-driver"
import { Inject } from "typedi"
import { Transaction } from "../models/transaction"
import { TransactionInput, TransactionInputCursor, PaginatedTransactionInputResponse } from "../models/transaction-input"
import { TransactionOutput, TransactionOutputCursor, PaginatedTransactionOutputResponse } from "../models/transaction-output"
import { Address } from "../models/address"
import { PaginationArgs } from "./pagination-args"
import { BlockHash } from "../models/block_hash"
import { MempoolTx } from "../mempool/mempool"
import { RpcVout } from "../rpc-client"
import { ScriptPubKey } from "../models/scriptpubkey"
import { LimitedCapacityClient } from "../limited-capacity-client"

@ArgsType()
class TransactionInputArgs extends PaginationArgs {

    @Field({ nullable: true })
    cursor: TransactionInputCursor

}

@ArgsType()
class TransactionOutputArgs extends PaginationArgs {

    @Field({ nullable: true })
    cursor: TransactionOutputCursor

}

@Resolver(of => Transaction)
export class TransactionResolver {

    constructor(
        @Inject("cassandra_client") private client: LimitedCapacityClient
    ) { }

    @FieldResolver(returns => BlockHash, { nullable: true, complexity: ({ childComplexity, args }) => 100 + childComplexity })
    async blockHash(@Root() transaction: Transaction,
    ): Promise<BlockHash> {
        if (transaction.height === undefined || transaction.height === null) return null
        const mempoolBlock = transaction.coin.mempool?.blockByHeight.get(transaction.height)
        if (mempoolBlock !== undefined) {
            return <BlockHash>{
                hash: mempoolBlock.rpcBlock.hash,
                height: mempoolBlock.rpcBlock.height,
                coin: transaction.coin
            }
        }
        const args: any[] = [transaction.height]
        const query: string = "SELECT * FROM " + transaction.coin.keyspace + ".longest_chain WHERE height=?"
        const resultSet: types.ResultSet = await this.client.execute(
            query,
            args,
            { prepare: true }
        )
        const res: BlockHash[] = resultSet.rows.map(row => {
            return <BlockHash>{
                hash: row.get('hash'),
                height: row.get('height'),
                coin: transaction.coin
            }
        })
        return res[0]
    }



    @FieldResolver(returns => PaginatedTransactionInputResponse, { nullable: false, complexity: ({ childComplexity, args }) => 100 + args.limit * childComplexity })
    async inputs(@Root() transaction: Transaction,
        @Args() { cursor, limit }: TransactionInputArgs
    ): Promise<PaginatedTransactionInputResponse> {
        const mempoolTx = transaction.coin.mempool?.txById.get(transaction.txid)
        if (mempoolTx !== undefined) {
            const res: TransactionInput[] = []
            const fromIndex = cursor === undefined ? 0 : cursor.spendingIndex + 1
            for (let spending_index = fromIndex; spending_index < mempoolTx.rpcTx.vin.length; spending_index++) {
                if (res.length == limit) {
                    return {
                        hasMore: true,
                        items: res
                    }
                }
                const rpcVin = mempoolTx.rpcTx.vin[spending_index]
                res.push(new TransactionInput({
                    coinbase: rpcVin.coinbase,
                    scriptSig: rpcVin.scriptSig,
                    sequence: rpcVin.sequence,
                    txid: rpcVin.txid,
                    vout: rpcVin.vout,
                    spendingTxid: mempoolTx.rpcTx.txid,
                    spendingIndex: spending_index,
                    coin: transaction.coin
                }))
            }
            return {
                hasMore: false,
                items: res
            }
        }
        let args: any[] = [transaction.txid]
        let query: string = "SELECT * FROM " + transaction.coin.keyspace + ".transaction_input WHERE spending_txid=?"
        if (cursor) {
            query += " AND spending_index > ?"
            args = args.concat([cursor.spendingIndex])
        }
        query += " LIMIT ?"
        args.push(limit + 1)
        const resultSet: types.ResultSet = await this.client.execute(
            query,
            args,
            { prepare: true, fetchSize: null }
        )
        const hasMore: boolean = resultSet.rows.length > limit
        if (hasMore) resultSet.rows.pop()

        const res: TransactionInput[] = resultSet.rows.map(row => {
            return new TransactionInput({
                coinbase: row.get('coinbase'),
                scriptSig: row.get('scriptsig'),
                sequence: row.get('sequence'),
                txid: row.get('txid'),
                vout: row.get('vout'),
                spendingTxid: row.get('spending_txid'),
                spendingIndex: row.get('spending_index'),
                coin: transaction.coin
            })
        })
        return {
            hasMore: hasMore,
            items: res
        }
    }

    @FieldResolver(returns => PaginatedTransactionOutputResponse, { nullable: false, complexity: ({ childComplexity, args }) => 100 + args.limit * childComplexity })
    async outputs(@Root() transaction: Transaction,
        @Args() { cursor, limit }: TransactionOutputArgs
    ): Promise<PaginatedTransactionOutputResponse> {
        const mempoolTx: MempoolTx = transaction.coin.mempool?.txById.get(transaction.txid)
        if (mempoolTx !== undefined) {
            const res: TransactionOutput[] = []
            const fromIndex = cursor === undefined ? 0 : cursor.n + 1
            for (let n = fromIndex; n < mempoolTx.rpcTx.vout.length; n++) {
                if (res.length == limit) {
                    return {
                        hasMore: true,
                        items: res
                    }
                }
                const rpcVout: RpcVout = mempoolTx.rpcTx.vout[n]
                const scriptpubkey: ScriptPubKey = new ScriptPubKey()// = rpcVout.scriptPubKey;
                scriptpubkey.asm = rpcVout.scriptPubKey.asm
                scriptpubkey.hex = rpcVout.scriptPubKey.hex
                scriptpubkey.reqSigs = rpcVout.scriptPubKey.reqSigs
                scriptpubkey.type = rpcVout.scriptPubKey.type
                if (rpcVout.scriptPubKey.addresses !== undefined && rpcVout.scriptPubKey.addresses !== null) {
                    scriptpubkey.addresses = rpcVout.scriptPubKey.addresses.map(address => new Address({ address: address, coin: transaction.coin }))
                }

                let spendingTxid: string
                let spendingIndex: number
                const spending_inpoint = transaction.coin.mempool.outpointToInpoint.get(mempoolTx.rpcTx.txid + n)
                if (spending_inpoint !== undefined) {
                    spendingTxid = spending_inpoint.spending_txid
                    spendingIndex = spending_inpoint.spending_index
                }

                res.push(new TransactionOutput({
                    txid: mempoolTx.rpcTx.txid,
                    n: n,
                    value: rpcVout.value,
                    scriptPubKey: scriptpubkey,
                    spendingTxid: spendingTxid,
                    spendingIndex: spendingIndex,
                    coin: transaction.coin
                }))
            }
            return {
                hasMore: false,
                items: res
            }
        }

        let args: any[] = [transaction.txid]
        let query: string = "SELECT * FROM " + transaction.coin.keyspace + ".transaction_output WHERE txid=?"
        if (cursor) {
            query += " AND n > ?"
            args = args.concat([cursor.n])
        }
        query += " LIMIT ?"
        args.push(limit + 1)
        const resultSet: types.ResultSet = await this.client.execute(
            query,
            args,
            { prepare: true, fetchSize: limit }
        )
        const hasMore: boolean = resultSet.rows.length > limit
        if (hasMore) resultSet.rows.pop()
        const res: TransactionOutput[] = resultSet.rows.map(row => {

            const n: number = row.get('n')
            const scriptpubkey = row.get('scriptpubkey')
            if (scriptpubkey.addresses !== undefined && scriptpubkey.addresses !== null) {
                scriptpubkey.addresses = scriptpubkey.addresses.map(address => new Address({ address: address, coin: transaction.coin }))
            }
            let spendingTxid: string = row.get('spending_txid')
            let spendingIndex: number = row.get('spending_index')
            if (spendingTxid === undefined || spendingTxid === null) {
                const spending_inpoint = transaction.coin.mempool?.outpointToInpoint.get(transaction.txid + n)
                if (spending_inpoint !== undefined) {
                    spendingTxid = spending_inpoint.spending_txid
                    spendingIndex = spending_inpoint.spending_index
                }
            }
            return new TransactionOutput({
                txid: transaction.txid,
                n: n,
                value: row.get('value'),
                scriptPubKey: scriptpubkey,
                spendingTxid: spendingTxid,
                spendingIndex: spendingIndex,
                coin: transaction.coin
            })
        })
        return {
            hasMore: hasMore,
            items: res
        }
    }

}