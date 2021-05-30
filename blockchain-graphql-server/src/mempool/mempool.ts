import { Readable } from "stream"
import { Subscriber } from "zeromq"
import { LimitedCapacityClient } from "../limited-capacity-client"
import { AddressBalance } from "../models/address-balance"
import { AddressTransaction } from "../models/address-transaction"
import { Coin } from "../models/coin"
import { Transaction } from "../models/transaction"
import { RpcBlock, RpcClient, RpcTx } from "../rpc-client"
import { BlockFetcher } from "./block-fetcher"
import { BlockHandler } from "./block-handler"
import { BlockInputDetailsFetcher } from "./block-input-details-fetcher"
import { TransactionInputDetailsFetcher, MempoolEvent3 } from "./transaction-input-details-fetcher"
import { BlockReader } from "./block-reader"
import { UnconfirmedTransactionFetcher } from "./unconfirmed-transaction-fetcher"
import { UnconfirmedTransactionWaiter } from "./unconfirmed-transaction-waiter"
import { UnconfirmedMempool } from "./unconfirmed_mempool"
import { ZmqParser } from "./zmq-parser"
import { TransactionInputDetailsWaiter } from "./transaction-input-details-waiter"

export class Mempool {
    public time: number
    public height: number
    public txById: Map<string, MempoolTx> = new Map();
    public blockByHash: Map<string, MempoolBlock> = new Map();
    public blockByHeight: Map<number, MempoolBlock> = new Map();
    public outpointToInpoint: Map<string, { spending_txid: string, spending_index: number }> = new Map();
    public addressBalances: Map<string, AddressBalance[]> = new Map();
    public addressTransactions: Map<string, AddressTransaction[]> = new Map();
    public unconfirmedMempool: UnconfirmedMempool = new UnconfirmedMempool();

    private blockReader: BlockReader
    private blockFetcher: BlockFetcher
    private blockInputFetcher: BlockInputDetailsFetcher
    private txInputFetcher: TransactionInputDetailsFetcher
    private txInputWaiter: TransactionInputDetailsWaiter
    private blockHandler: BlockHandler
    private zmqParser: ZmqParser
    private unconfirmedTxidFetcher: UnconfirmedTransactionFetcher
    private unconfirmedTransactionWaiter: UnconfirmedTransactionWaiter

    private socket: Subscriber

    constructor(public rpcClient: RpcClient, private client: LimitedCapacityClient, private coin: Coin) {
        this.blockReader = new BlockReader(this.rpcClient, this.coin, 10, this)
        this.blockFetcher = new BlockFetcher(this.rpcClient, this)
        const m1 = new Map()
        const m2 = new Map()
        this.blockInputFetcher = new BlockInputDetailsFetcher(this.client, this.rpcClient, this.coin, this, m1, m2)
        this.txInputFetcher = new TransactionInputDetailsFetcher(this.client, this.rpcClient, this.coin, this, m1, m2)
        this.txInputWaiter = new TransactionInputDetailsWaiter()
        this.blockHandler = new BlockHandler(this.client, this.coin, this)
        this.zmqParser = new ZmqParser()
        this.unconfirmedTxidFetcher = new UnconfirmedTransactionFetcher(this.rpcClient, this)
        this.unconfirmedTransactionWaiter = new UnconfirmedTransactionWaiter()
    }

    private timeout: NodeJS.Timeout

    public async start(): Promise<void> {
        console.log("Starting mempool")
        this.socket = new Subscriber()
        this.zmqParser.pipe(this.unconfirmedTxidFetcher, { end: false })
        this.unconfirmedTxidFetcher.pipe(this.unconfirmedTransactionWaiter, { end: false }).pipe(this.txInputFetcher, { end: false }).pipe(this.txInputWaiter, { end: false }).pipe(this.blockHandler)
        const socketStream = Readable.from(this.socket)
        socketStream.pipe(this.zmqParser, { end: false })
        this.blockReader.pipe(this.blockFetcher, { end: false }).pipe(this.blockInputFetcher, { end: false }).pipe(this.blockHandler)
        await new Promise((resolve) => setTimeout(resolve, 10000))
        const txids: string[] = await this.rpcClient.getRawMempool()
        txids.forEach(txid => this.unconfirmedTxidFetcher.write(<MempoolEvent3>{ type: "hashtx", txid: txid }))
        if (this.coin.zmq_addresses && this.coin.zmq_addresses.length > 0) {
            this.socket.connect(this.coin.zmq_addresses[0])
            this.socket.subscribe("hashtx")
        }
        this.timeout = setInterval(() => {
            console.log(`${this.coin.name} streams
            socketStream: R=${socketStream.readableLength}
            blockReader: R=${this.blockReader.readableLength} 
            blockFetcher: R=${this.blockFetcher.readableLength}, W=${this.blockFetcher.writableLength}
            zmqParser: R=${this.zmqParser.readableLength}, W=${this.zmqParser.writableLength}
            unconfirmedTxidFetcher: R=${this.unconfirmedTxidFetcher.readableLength}, W=${this.unconfirmedTxidFetcher.writableLength}
            unconfirmedTransactionWaiter: R=${this.unconfirmedTxidFetcher.readableLength}, W=${this.unconfirmedTxidFetcher.writableLength}
            blockHandler: W=${this.blockHandler.writableLength}`)
            //console.log(this.coin.name + " streams - blockReader: R=" + this.blockReader.readableLength + " \n blockFetcher: R=" + this.blockFetcher.readableLength + ", W=" + this.blockFetcher.writableLength);
        }, 5 * 60000)
    }

    public async stop(): Promise<void> {
        clearInterval(this.timeout)
        console.log("Stopping " + this.coin.name + " mempool")
        if (this.socket !== undefined) this.socket.close()
        this.blockReader.destroy()
        this.blockFetcher.destroy()
        this.blockInputFetcher.destroy()
        this.blockHandler.destroy()
        this.zmqParser.destroy()
        this.unconfirmedTxidFetcher.destroy()
        this.unconfirmedTransactionWaiter.destroy()
    }


}

export class MempoolTx {

    constructor(public rpcTx: RpcTx, public height?: number, public txN?: number) {
    }

    fee: number

    public toGraphQL(coin: Coin): Transaction {
        return <Transaction>{
            txid: this.rpcTx.txid,
            lockTime: this.rpcTx.locktime,
            size: this.rpcTx.size,
            version: this.rpcTx.version,
            height: this.height,
            txN: this.txN,
            fee: this.fee,
            inputCount: this.rpcTx.vin.length,
            outputCount: this.rpcTx.vout.length,
            coin: coin
        }
    }
}

export class MempoolBlock {

    constructor(public rpcBlock: RpcBlock) {
        this.tx = rpcBlock.tx.map((tx, txN) => {
            return new MempoolTx(tx, rpcBlock.height, txN)
        })
    }

    tx: MempoolTx[]
}