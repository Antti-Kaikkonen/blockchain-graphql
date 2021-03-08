import { Readable } from "stream";
import { Subscriber } from "zeromq";
import { LimitedCapacityClient } from "../limited-capacity-client";
import { AddressBalance } from "../models/address-balance";
import { AddressTransaction } from "../models/address-transaction";
import { Coin } from "../models/coin";
import { Transaction } from "../models/transaction";
import { RpcBlock, RpcClient, RpcTx } from "../rpc-client";
import { BlockFetcher } from "./block-fetcher";
import { BlockHandler } from "./block-handler";
import { BlockInputDetailsFetcher, MempoolEvent3 } from "./block-input-details-fetcher";
import { BlockReader } from "./block-reader";
import { UnconfirmedTransactionFetcher } from "./unconfirmed-transaction-fetcher";
import { UnconfirmedTransactionWaiter } from "./unconfirmed-transaction-waiter";
import { UnconfirmedMempool } from "./unconfirmed_mempool";
import { ZmqParser } from "./zmq-parser";

export class Mempool {
    public time: number;
    public height: number;
    public txById: Map<string, MempoolTx> = new Map();
    public blockByHash: Map<string, MempoolBlock> = new Map();
    public blockByHeight: Map<number, MempoolBlock> = new Map();
    public outpointToInpoint: Map<string, { spending_txid: string, spending_index: number }> = new Map();
    public addressBalances: Map<String, AddressBalance[]> = new Map();
    public addressTransactions: Map<string, AddressTransaction[]> = new Map();
    public unconfirmedMempool: UnconfirmedMempool = new UnconfirmedMempool();

    private blockReader: BlockReader;
    private blockFetcher: BlockFetcher;
    private inputFetcher: BlockInputDetailsFetcher;
    private blockHandler: BlockHandler;
    private zmqParser: ZmqParser;
    private unconfirmedTxidFetcher: UnconfirmedTransactionFetcher;
    private unconfirmedTransactionWaiter: UnconfirmedTransactionWaiter;

    private socket: Subscriber;

    constructor(public rpcClient: RpcClient, private client: LimitedCapacityClient, private coin: Coin) {
        this.blockReader = new BlockReader(this.rpcClient, this.coin, 10, this);
        this.blockFetcher = new BlockFetcher(this.rpcClient, this);
        this.inputFetcher = new BlockInputDetailsFetcher(this.client, this.rpcClient, this.coin, this);
        this.blockHandler = new BlockHandler(this.client, this.coin, this);
        this.zmqParser = new ZmqParser();
        this.unconfirmedTxidFetcher = new UnconfirmedTransactionFetcher(this.rpcClient, this);
        this.unconfirmedTransactionWaiter = new UnconfirmedTransactionWaiter();
    }

    public async start(): Promise<void> {
        console.log("Starting mempool");
        this.socket = new Subscriber();
        this.zmqParser.pipe(this.unconfirmedTxidFetcher, { end: false });
        this.unconfirmedTxidFetcher.pipe(this.unconfirmedTransactionWaiter, { end: false }).pipe(this.inputFetcher, { end: false });
        Readable.from(this.socket).pipe(this.zmqParser, { end: false });
        this.blockReader.pipe(this.blockFetcher).pipe(this.inputFetcher).pipe(this.blockHandler);
        await new Promise((resolve) => setTimeout(resolve, 10000));
        let txids: string[] = await this.rpcClient.getRawMempool();
        txids.forEach(txid => this.unconfirmedTxidFetcher.write(<MempoolEvent3>{ type: "hashtx", txid: txid }));
        if (this.coin.zmq_addresses && this.coin.zmq_addresses.length > 0) {
            this.socket.connect(this.coin.zmq_addresses[0]);
            this.socket.subscribe("hashtx");
        }
    }

    public stop(): void {
        console.log("Stopping mempool");
        if (this.socket !== undefined) this.socket.close();
        this.blockReader.destroy();
        this.blockFetcher.destroy();
        this.inputFetcher.destroy();
        this.blockHandler.destroy();
        this.zmqParser.destroy();
        this.unconfirmedTxidFetcher.destroy();
        this.unconfirmedTransactionWaiter.destroy();
    }


}

export class MempoolTx {

    constructor(public rpcTx: RpcTx, public height?: number, public txN?: number) {
    }

    fee: number;

    public toGraphQL(coin: Coin): Transaction {
        return <Transaction>{
            txid: this.rpcTx.txid,
            lockTime: this.rpcTx.locktime,
            size: this.rpcTx.size,
            version: this.rpcTx.version,
            height: this.height,
            txN: this.txN,
            fee: this.fee,
            coin: coin
        };
    }
}

export class MempoolBlock {

    constructor(public rpcBlock: RpcBlock) {
        this.tx = rpcBlock.tx.map((tx, txN) => {
            return new MempoolTx(tx, rpcBlock.height, txN);
        });
    }

    tx: MempoolTx[];
}