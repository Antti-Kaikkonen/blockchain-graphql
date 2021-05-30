import { types } from "cassandra-driver"
import { Arg, Args, ArgsType, Field, FieldResolver, Int, Mutation, Query, Resolver, Root } from "type-graphql"
import { Inject } from "typedi"
import { LimitedCapacityClient } from "../limited-capacity-client"
import { MempoolBlock, MempoolTx } from "../mempool/mempool"
import { Address } from "../models/address"
import { AddressCluster } from "../models/address-cluster"
import { AddressClusterRichlist, AddressClusterRichlistCursor, PaginatedAddressClusterRichlistResponse } from "../models/address-cluster-richlist"
import { Block } from "../models/block"
import { BlockHash, BlockHashCursor, PaginatedAddressBlockHashtResponse } from "../models/block_hash"
import { Coin } from "../models/coin"
import { ConfirmedTransaction } from "../models/confirmed-transaction"
import { Date as DateModel } from "../models/date"
import { MempoolModel } from "../models/mempool-model"
import { ScriptPubKey } from "../models/scriptpubkey"
import { SendRawTransactionResult } from "../models/sendrawtransactionresult"
import { Transaction } from "../models/transaction"
import { TransactionInput } from "../models/transaction-input"
import { TransactionOutput } from "../models/transaction-output"
import { RpcVin, RpcVout } from "../rpc-client"
import { PaginationArgs } from "./pagination-args"

@ArgsType()
class ClusterRichlistArgs extends PaginationArgs {

    @Field({ nullable: true })
    cursor: AddressClusterRichlistCursor

}

@ArgsType()
class BlockHashArgs extends PaginationArgs {

    @Field({ nullable: true })
    cursor: BlockHashCursor

}

@Resolver(of => Coin)
export class CoinResolver {

    constructor(
        @Inject("cassandra_client") private client: LimitedCapacityClient,
        @Inject("coins_keyspace") private coins_keyspace: string,
        @Inject("coins") private available_coins: Map<string, Coin>
    ) {
        this.coins()//updates lastCoinCount;
    }

    private static lastCoinCount = 1

    public static CLUSTER_RICHLIST_BIN_COUNT = 100
    static CLUSTER_RICHLIST_BINS: number[] = Array.from(new Array(CoinResolver.CLUSTER_RICHLIST_BIN_COUNT).keys())

    @Query(returns => [Coin], { nullable: false, complexity: ({ childComplexity, args }) => 100 + CoinResolver.lastCoinCount * childComplexity })
    async coins(): Promise<Coin[]> {
        CoinResolver.lastCoinCount = this.available_coins.size
        return Array.from(this.available_coins.values())
    }

    @Query(returns => Coin, { nullable: true, complexity: 100 })
    async coin(@Arg("name") name: string): Promise<Coin> {
        return this.available_coins.get(name)
    }

    @FieldResolver(returns => DateModel, { nullable: false, complexity: 1 })
    async date(
        @Root() coin: Coin,
        @Arg("date") date: string
    ): Promise<DateModel> {
        return <DateModel>{
            date: date,
            coin: coin
        }
    }

    @FieldResolver(returns => MempoolModel, { nullable: false, complexity: 1 })
    async mempool(
        @Root() coin: Coin
    ): Promise<MempoolModel> {
        return <MempoolModel>{ coin: coin }
    }

    @FieldResolver(returns => Address, { nullable: false, complexity: 1 })
    async address(@Root() coin: Coin, @Arg("address") address: string): Promise<Address> {
        const res = new Address({ address: address, coin: coin })
        return res
    }

    @FieldResolver(returns => Transaction, { nullable: true, complexity: ({ childComplexity, args }) => 100 + childComplexity })
    async transaction(@Root() coin: Coin, @Arg("txid") txid: string): Promise<Transaction> {
        const mempoolTransaction = coin.mempool?.txById.get(txid)
        if (mempoolTransaction !== undefined) {
            return mempoolTransaction.toGraphQL(coin)
        }
        const args: any[] = [txid]
        const query: string = "SELECT * FROM " + coin.keyspace + ".transaction WHERE txid=?"
        const resultSet: types.ResultSet = await this.client.execute(
            query,
            args,
            { prepare: true }
        )
        const res: Transaction[] = resultSet.rows.map(row => {
            return <Transaction>{
                txid: row.get('txid'),
                lockTime: row.get('locktime'),
                size: row.get('size'),
                version: row.get('version'),
                height: row.get('height'),
                txN: row.get("tx_n"),
                fee: row.get("fee"),
                inputCount: row.get("input_count"),
                outputCount: row.get("output_count"),
                coin: coin
            }
        })
        return res[0]
    }


    @FieldResolver(returns => TransactionInput, { nullable: true, complexity: ({ childComplexity, args }) => 100 + childComplexity })
    async transactionInput(@Root() coin: Coin,
        @Arg("spendingTxid", type => String) spendingTxid: string,
        @Arg("spendingIndex", type => Int) spendingIndex: number
    ): Promise<TransactionInput> {
        const mempoolTx: MempoolTx = coin.mempool?.txById.get(spendingTxid)
        if (mempoolTx !== undefined) {
            const spending_input: RpcVin = mempoolTx.rpcTx.vin[spendingIndex]
            if (spending_input === undefined) return null
            const vin: TransactionInput = new TransactionInput({
                coinbase: spending_input.coinbase,
                scriptSig: spending_input.scriptSig,
                sequence: spending_input.sequence,
                txid: spending_input.txid,
                vout: spending_input.vout,
                spendingTxid: spendingTxid,
                spendingIndex: spendingIndex,
                coin: coin
            })
            return vin
        }

        const args: any[] = [spendingTxid, spendingIndex]
        const query: string = "SELECT * FROM " + coin.keyspace + ".transaction_input WHERE spending_txid=? AND spending_index=?"
        const resultSet: types.ResultSet = await this.client.execute(
            query,
            args,
            { prepare: true }
        )
        const res: TransactionInput[] = resultSet.rows.map(row => {
            const vin: TransactionInput = new TransactionInput({
                coinbase: row.get("coinbase"),
                scriptSig: row.get("scriptsig"),
                sequence: row.get('sequence'),
                txid: row.get('txid'),
                vout: row.get('vout'),
                spendingTxid: row.get('spending_txid'),
                spendingIndex: row.get('spending_index'),
                coin: coin
            })
            return vin
        })
        return res[0]
    }


    @FieldResolver(returns => TransactionOutput, { nullable: true, complexity: ({ childComplexity, args }) => 100 + childComplexity })
    async transactionOutput(@Root() coin: Coin,
        @Arg("txid", type => String) txid: string,
        @Arg("n", type => Int) n: number
    ): Promise<TransactionOutput> {
        const mempoolTx = coin.mempool?.txById.get(txid)
        if (mempoolTx !== undefined) {
            const rpcVout: RpcVout = mempoolTx.rpcTx.vout[n]
            if (rpcVout === undefined) return null
            const scriptpubkey: ScriptPubKey = new ScriptPubKey()// = rpcVout.scriptPubKey;
            scriptpubkey.asm = rpcVout.scriptPubKey.asm
            scriptpubkey.hex = rpcVout.scriptPubKey.hex
            scriptpubkey.reqSigs = rpcVout.scriptPubKey.reqSigs
            scriptpubkey.type = rpcVout.scriptPubKey.type
            if (rpcVout.scriptPubKey.addresses !== undefined && rpcVout.scriptPubKey.addresses !== null) {
                scriptpubkey.addresses = rpcVout.scriptPubKey.addresses.map(address => new Address({ address: address, coin: coin }))
            }
            const vout: TransactionOutput = new TransactionOutput({
                txid: mempoolTx.rpcTx.txid,
                n: rpcVout.n,
                value: rpcVout.value,
                scriptPubKey: scriptpubkey,
                coin: coin
            })
            const spending_inpoint = coin.mempool.outpointToInpoint.get(vout.txid + vout.n)
            if (spending_inpoint !== null) {
                vout.spendingTxid = spending_inpoint.spending_txid
                vout.spendingIndex = spending_inpoint.spending_index
            }
            return vout
        }
        const args: any[] = [txid, n]
        const query: string = "SELECT * FROM " + coin.keyspace + ".transaction_output WHERE txid=? AND n=?"
        const resultSet: types.ResultSet = await this.client.execute(
            query,
            args,
            { prepare: true }
        )
        const res: TransactionOutput[] = resultSet.rows.map(row => {
            const scriptpubkey = row.get('scriptpubkey')
            if (scriptpubkey.addresses !== undefined && scriptpubkey.addresses !== null) {
                scriptpubkey.addresses = scriptpubkey.addresses.map(address => new Address({ address: address, coin: coin }))
            }
            let spendingTxid: string = row.get('spending_txid')
            let spendingIndex: number = row.get('spending_index')
            if (spendingTxid === undefined || spendingTxid === null) {
                const spending_inpoint = coin.mempool?.outpointToInpoint.get(txid + n)
                if (spending_inpoint !== null) {
                    spendingTxid = spending_inpoint.spending_txid
                    spendingIndex = spending_inpoint.spending_index
                }
            }
            return new TransactionOutput({
                txid: txid,
                n: n,
                value: row.get('value'),
                scriptPubKey: scriptpubkey,
                spendingTxid: spendingTxid,
                spendingIndex: spendingIndex,
                coin: coin
            })
        })
        return res[0]
    }

    @FieldResolver(returns => ConfirmedTransaction, { nullable: true, complexity: ({ childComplexity, args }) => 100 + childComplexity })
    async confirmedTransaction(
        @Root() coin: Coin,
        @Arg("height", type => Int) height: number,
        @Arg("tx_n", type => Int) tx_n: number
    ): Promise<ConfirmedTransaction> {
        const mempoolBlock: MempoolBlock = coin.mempool?.blockByHeight.get(height)
        if (mempoolBlock !== undefined) {
            const mempoolTx: MempoolTx = mempoolBlock.tx[tx_n]
            return <ConfirmedTransaction>{
                height: mempoolTx.height,
                txN: mempoolTx.txN,
                txid: mempoolTx.rpcTx.txid,
                coin: coin
            }
        }
        const args: any[] = [height, tx_n]
        const query: string = "SELECT * FROM " + coin.keyspace + ".confirmed_transaction WHERE height=? AND tx_n=?"
        const resultSet: types.ResultSet = await this.client.execute(
            query,
            args,
            { prepare: true }
        )
        const res: ConfirmedTransaction[] = resultSet.rows.map(row => {
            return <ConfirmedTransaction>{
                height: row.get('height'),
                txN: row.get('tx_n'),
                txid: row.get("txid"),
                coin: coin
            }
        })
        return res[0]
    }

    @FieldResolver(returns => PaginatedAddressBlockHashtResponse, { nullable: false, complexity: ({ childComplexity, args }) => args.limit * childComplexity })
    async blocks(
        @Root() coin: Coin,
        @Args() { limit, cursor }: BlockHashArgs
    ): Promise<PaginatedAddressBlockHashtResponse> {
        const lastBlockHeight: number = coin.mempool?.height
        if (lastBlockHeight === undefined) {
            return { hasMore: false, items: [] }
        }
        const fromHeight = (cursor?.height === undefined || cursor?.height === null) ? lastBlockHeight : Math.min(cursor.height - 1, lastBlockHeight)
        const res: Promise<BlockHash>[] = []
        for (let currentHeight: number = fromHeight; currentHeight >= Math.max(fromHeight - limit + 1, 0); currentHeight--) {
            res.push(this.blockByHeight(coin, currentHeight))
        }
        return {
            hasMore: fromHeight - limit + 1 > 0,
            items: await Promise.all(res)
        }
        /*return new Promise<PaginatedAddressBlockHashtResponse>(async (resolve, reject) => {
            resolve({
                hasMore: fromHeight - limit + 1 > 0,
                items: await Promise.all(res)
            })
        });*/
    }

    @FieldResolver(returns => BlockHash, { nullable: true, complexity: ({ childComplexity, args }) => 100 + childComplexity })
    async blockByHeight(
        @Root() coin: Coin,
        @Arg("height", type => Int) height: number
    ): Promise<BlockHash> {
        const mempoolBlock: MempoolBlock = coin.mempool?.blockByHeight.get(height)
        if (mempoolBlock !== undefined) {
            return <BlockHash>{
                hash: mempoolBlock.rpcBlock.hash,
                height: mempoolBlock.rpcBlock.height,
                coin: coin
            }
        }
        const args: any[] = [height]
        const query: string = "SELECT * FROM " + coin.keyspace + ".longest_chain WHERE height=?"
        const resultSet: types.ResultSet = await this.client.execute(
            query,
            args,
            { prepare: true }
        )
        const res: BlockHash[] = resultSet.rows.map(row => {
            return <BlockHash>{
                hash: row.get('hash'),
                height: row.get('height'),
                coin: coin
            }
        })
        return res[0]
    }

    @FieldResolver(returns => Block, { nullable: true, complexity: ({ childComplexity, args }) => 100 + childComplexity })
    async block(
        @Root() coin: Coin,
        @Arg("hash") hash: string
    ): Promise<Block> {
        const mempooBlock: MempoolBlock = coin.mempool?.blockByHash.get(hash)
        if (mempooBlock !== undefined) {
            return <Block>{
                height: mempooBlock.rpcBlock.height,
                hash: mempooBlock.rpcBlock.hash,
                size: mempooBlock.rpcBlock.size,
                version: mempooBlock.rpcBlock.version,
                versionHex: mempooBlock.rpcBlock.versionHex,
                merkleRoot: mempooBlock.rpcBlock.merkleroot,
                time: new Date(mempooBlock.rpcBlock.time * 1000),
                medianTime: mempooBlock.rpcBlock.mediantime,
                nonce: mempooBlock.rpcBlock.nonce,
                bits: mempooBlock.rpcBlock.bits,
                difficulty: mempooBlock.rpcBlock.difficulty,
                chainWork: mempooBlock.rpcBlock.chainwork,
                previousBlockHash: mempooBlock.rpcBlock.previousblockhash,
                txCount: mempooBlock.tx.length,
                coin: coin
            }
        }
        const args: any[] = [hash]
        const query: string = "SELECT * FROM " + coin.keyspace + ".block WHERE hash=?"
        const resultSet: types.ResultSet = await this.client.execute(
            query,
            args,
            { prepare: true }
        )
        const res: Block[] = resultSet.rows.map(row => {
            return <Block>{
                height: row.get('height'),
                hash: row.get('hash'),
                size: row.get("size"),
                version: row.get('version'),
                versionHex: row.get("versionhex"),
                merkleRoot: row.get("merkleroot"),
                time: row.get("time"),
                medianTime: row.get("mediantime"),
                nonce: row.get("nonce"),
                bits: row.get("bits"),
                difficulty: row.get("difficulty"),
                chainWork: row.get("chainwork"),
                previousBlockHash: row.get("previousblockhash"),
                txCount: row.get("tx_count"),
                coin: coin
            }
        })
        return res[0]
    }

    @FieldResolver(returns => PaginatedAddressClusterRichlistResponse, { nullable: false, complexity: ({ childComplexity, args }) => 100 + args.limit * childComplexity })
    async clusterRichlist(@Root() coin: Coin,
        @Args() { limit, cursor }: ClusterRichlistArgs
    ): Promise<PaginatedAddressClusterRichlistResponse> {
        let args: any[] = [CoinResolver.CLUSTER_RICHLIST_BINS]
        let query: string = "SELECT balance, cluster_id FROM " + coin.keyspace + ".cluster_richlist WHERE bin IN ?"
        if (cursor) {
            query += " AND (balance, cluster_id) < (?, ?)"
            args = args.concat([cursor.balance, cursor.clusterId])
        }
        args.push(limit + 1)
        query += " ORDER BY balance DESC, cluster_id DESC LIMIT ?"
        const resultSet: types.ResultSet = await this.client.execute(
            query,
            args,
            { prepare: true, fetchSize: null }
        )
        const hasMore: boolean = resultSet.rows.length > limit
        if (hasMore) resultSet.rows.pop()
        const res: AddressClusterRichlist[] = resultSet.rows.map(row => {
            return <AddressClusterRichlist>{
                balance: row.get("balance"),
                cluster: <AddressCluster>{
                    coin: coin,
                    clusterId: row.get("cluster_id")
                }
            }
        })
        return {
            hasMore: hasMore,
            items: res
        }
    }

    @Mutation(returns => SendRawTransactionResult, { nullable: false, complexity: ({ childComplexity, args }) => 20000 + childComplexity })
    async sendRawTransaction(@Arg("coinName") coinName: string, @Arg("hexString") hexString: string): Promise<SendRawTransactionResult> {
        const coin = this.available_coins.get(coinName)
        if (coin === undefined) throw new Error("Coin " + coinName + " not available.")
        return { txid: await coin.mempool.rpcClient.sendRawTransaction(hexString) }
    }
}