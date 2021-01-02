
import { types } from "cassandra-driver";
import { LimitedCapacityClient } from "./limited-capacity-client";
import { AddressBalance } from "./models/address-balance";
import { AddressTransaction } from "./models/address-transaction";
import { Coin } from "./models/coin";
import { Transaction } from "./models/transaction";
import { RpcBlock, RpcBlockNoTx, RpcClient, RpcTx, RpcVin } from "./rpc-client";

const BLOCKS_COUNT: number = 10;

export class MempoolTx extends RpcTx {

    constructor(rpcTx: RpcTx, public height: number, public txN: number) {
        super();
        Object.assign(this, rpcTx);
    }

    fee: number;
    
    public toGraphQL(coin: Coin): Transaction {
        let tx: Transaction = new Transaction();
        tx.txid = this.txid;
        tx.locktime = this.locktime;
        tx.size = this.size;
        tx.version = this.version;
        tx.height = this.height;
        tx.txN = this.txN;
        tx.fee = this.fee;
        tx.coin = coin;
        return tx;
    }
}

export class MempoolBlock extends RpcBlockNoTx {

    constructor(rpcBlock: RpcBlock) {
        super();
        this.hash = rpcBlock.hash;
        this.height = rpcBlock.height;
        this.mediantime = rpcBlock.mediantime;
        this.merkleroot = rpcBlock.merkleroot;
        this.nTx = rpcBlock.nTx;
        this.nextblockhash = rpcBlock.nextblockhash;
        this.nonce = rpcBlock.nonce;
        this.previousblockhash = rpcBlock.previousblockhash;
        this.size = rpcBlock.size;
        this.time = rpcBlock.time;
        this.tx = rpcBlock.tx.map((tx, txN) => {
            return new MempoolTx(tx, rpcBlock.height, txN);
        });
        this.version = rpcBlock.version;
        this.versionHex = rpcBlock.versionHex;
    }

    tx: MempoolTx[];
}

export class Mempool {

    constructor(private rpcClient: RpcClient, private client: LimitedCapacityClient, private coin: Coin) {
    }
    
    public time: number;
    public height: number;
    public txById : Map<String, MempoolTx> = new Map();
    public blockByHash : Map<String, MempoolBlock> = new Map();
    public blockByHeight : Map<Number, MempoolBlock> = new Map();
    public outpointToInpoint: Map<string, {spending_txid: string, spending_index: number}> = new Map();
    public addressBalances : Map<String, AddressBalance[]> = new Map();
    public addressTransactions : Map<String, AddressTransaction[]> = new Map();

    private timeout: NodeJS.Timeout;

    private async updater() {
        try {
            await this.update();
        } catch(err) {
            console.log("Error updating "+this.coin.name+" mempool:", err);
        }
        this.timeout = setTimeout(() => {
            this.updater();
        }, 1000);
    }

    public async start(): Promise<void> {
        await this.updater();
    }


    public stop(): void {
        clearTimeout(this.timeout);
    }

    private blocksTxDetails(blocks: MempoolBlock[]): Promise<TxDetails[][]> {
        return Promise.all(blocks.map(block => this.blockTxDetails(block)));
    }

    private async blockTxDetails(block: MempoolBlock): Promise<TxDetails[]> {
        block.tx.forEach((tx, tx_n) => {
            this.txById.set(tx.txid, tx);
        });
        try {
            return await Promise.all(block.tx.map(tx => this.txDetails(tx)));
        } catch(error) {
            block.tx.forEach(tx => {
                this.txById.delete(tx.txid);
            });
            throw error;
        }
    }

    private async txDetails(tx: MempoolTx): Promise<TxDetails> {
        return new Promise(async (resolve, reject) => {
            let fee: number = 0;
            let addressDeltas: Map<string, number> = new Map();

            tx.vout.forEach(vout => {
                let valueSats = Math.round(vout.value*1e8);
                if (vout.scriptPubKey.addresses !== undefined && vout.scriptPubKey.addresses !== null && vout.scriptPubKey.addresses.length === 1) {
                    let address: string = vout.scriptPubKey.addresses[0];
                    let oldValue = addressDeltas.get(address);
                    addressDeltas.set(address, oldValue === undefined ? valueSats : oldValue + valueSats);
                }
                if (tx.txN !== 0) fee -= valueSats;
            });
            if (tx.txN !== 0 && tx.vin.length > 0) {
                let pending_promises = tx.vin.length;
                tx.vin.forEach(async (vin, spending_index) => {
                    let inputDetails: {address: string, value: number};
                    try {
                        inputDetails = await this.getInputDetails(vin, spending_index, tx.txid);
                    } catch(error) {
                        reject(error);
                        return;
                    }
                    let valueSats = Math.round(inputDetails.value*1e8);
                    fee+=valueSats;
                    if (inputDetails.address !== undefined) {
                        let oldValue = addressDeltas.get(inputDetails.address);
                        addressDeltas.set(inputDetails.address, oldValue === undefined ? -valueSats : oldValue - valueSats);
                    }
                    pending_promises--;
                    if (pending_promises === 0) {
                        resolve({
                            fee: fee/1e8,
                            addressDeltas: addressDeltas
                        });
                    }
                });
            } else {
                resolve({
                    fee: fee/1e8,
                    addressDeltas: addressDeltas
                });
            }
        });
    }

    private async getInputDetails(vin: RpcVin, spending_index: number, spending_txid: string): Promise<{address: string, value: number}> {
        return new Promise<{address: string, value: number}>(async (resolve, reject) => {
            try {
                let address: string;
                this.outpointToInpoint.set(vin.txid+vin.vout, {spending_txid: spending_txid, spending_index: spending_index});
                let mempool_tx = this.txById.get(vin.txid);
                if (mempool_tx !== undefined) {
                    let spent_output = mempool_tx.vout[vin.vout];
                    if (spent_output.scriptPubKey.addresses !== undefined && spent_output.scriptPubKey.addresses !== null && spent_output.scriptPubKey.addresses.length === 1) {
                        address = spent_output.scriptPubKey.addresses[0];
                    }
                    resolve({
                        address: address,
                        value: spent_output.value
                    });
                } else {
                    let res: types.ResultSet = await this.client.execute("SELECT value, scriptpubkey.addresses FROM "+this.coin.keyspace+".transaction_output WHERE txid = ? AND n=?;", [vin.txid, vin.vout], {prepare: true});
                    if (res.rows.length === 0) {
                        reject(this.coin.name+" output "+vin.txid+"-"+vin.vout+" was not found in db. Make sure your db is synchronized with the blockchain.");
                    } else {
                        res.rows.forEach(row => {
                            let value: number = row.get("value");
                            let addresses: string[] = row.get("scriptpubkey.addresses");
                            if (addresses !== undefined && addresses !== null && addresses.length === 1) {
                                address = addresses[0];
                            }
                            resolve({
                                address: address,
                                value: value
                            });
                        });
                    }
                }
            } catch(error) {
                throw error;
            }
        });
    }

    private async getDbBlockTimestamp(height: number): Promise<number> {
        try {
            let res = await this.client.execute("SELECT hash FROM "+this.coin.keyspace+".longest_chain WHERE height = ?;", [height], {prepare: true});
            let hash: string;
            res.rows.forEach(row => {
                hash = row.get("hash");
            });
            if (hash === undefined) throw new Error("Failed to get block hash for height "+height);
            let res2 = await this.client.execute("SELECT time FROM "+this.coin.keyspace+".block WHERE hash = ?;", [hash], {prepare: true});
            let time: number;
            res2.rows.forEach(row => {
                time = row.get("time");
            });
            if (time === undefined) throw new Error("Failed to get time for block "+hash+" at height "+height);
            return time;
        } catch(error) {
            throw error;
        }
    }

    private async getDbAddressBalance(address: string, beforeTimestamnp: number): Promise<number> {//returns value in satoshis
        try {
            let res = await this.client.execute("SELECT balance FROM "+this.coin.keyspace+".address_balance WHERE address = ? AND timestamp < ? LIMIT 1;", [address, beforeTimestamnp], {prepare: true});
            let balance: number;
            res.rows.forEach(row => {
                balance = new Number(row.get("balance")).valueOf();
            });
            return balance;
        } catch(error) {
            throw error;
        }
    }

    private deleteOrphanedAddressTransactions(block: MempoolBlock): void {
        this.addressBalances.forEach((balances: AddressBalance[], address: string) => {
            while (balances.length > 0 && balances[balances.length-1].timestamp.getTime() === block.time*1000) {
                balances.pop();
            }
            if (balances.length === 0) this.addressBalances.delete(address);
        });
        this.addressTransactions.forEach((txs: AddressTransaction[], address: string) => {
            while (txs.length > 0 && txs[txs.length-1].timestamp.getTime() === block.time*1000) {
                txs.pop();
            }
            if (txs.length === 0) this.addressTransactions.delete(address);
        });
    }

    private deleteExpiredAddressTransactions(block: MempoolBlock): void {
        this.addressBalances.forEach((balances: AddressBalance[], address: string) => {
            while (balances.length > 0 && balances[0].timestamp.getTime() === block.time*1000) {
                balances.shift();
            }
            if (balances.length === 0) this.addressBalances.delete(address);
        });
        this.addressTransactions.forEach((txs: AddressTransaction[], address: string) => {
            while (txs.length > 0 && txs[0].timestamp.getTime() === block.time*1000) {
                txs.shift();
            }
            if (txs.length === 0) this.addressTransactions.delete(address);
        });
    }

    private deleteBlock(block: MempoolBlock): void {
        this.blockByHeight.delete(block.height);
        this.blockByHash.delete(block.hash);
        block.tx.forEach(tx => {
            this.txById.delete(tx.txid); 
        });
    }

    private async blockAddressBalances(block: MempoolBlock, blockAddressDeltas: Map<string, number>): Promise<{address: string, balance: number}[]> {
        return Promise.all(Array.from(blockAddressDeltas.entries()).map(async ([address, delta]) => {
            try {
                let oldBalances = this.addressBalances.get(address);
                let oldBalance: number;
                if (oldBalances === undefined) {
                    //this.addressBalances.set(address, []);
                    oldBalance = await this.getDbAddressBalance(address, block.time*1000);
                    if (oldBalance === undefined) oldBalance = 0;
                } else {
                    oldBalance = Math.round(oldBalances[oldBalances.length-1].balance * 1e8);
                }
                return {address: address, balance: (oldBalance+delta)/1e8};
            } catch(error) {
                throw error;
            }
        }));
    }


    private async updateAddressTransactions(block: MempoolBlock, blockTxDetails: TxDetails[], blockAddressDeltas: Map<string, number>): Promise<void> {
        for (let tx_n = 0; tx_n < block.tx.length; tx_n++) {//update address transactions
            let txDetails = blockTxDetails[tx_n];
            txDetails.addressDeltas.forEach((delta: number, address: string) => {
                let oldDelta = blockAddressDeltas.get(address);
                blockAddressDeltas.set(address, oldDelta === undefined ? delta: oldDelta + delta);
                let addressTxs = this.addressTransactions.get(address);
                if (addressTxs === undefined) {
                    addressTxs = [];
                    this.addressTransactions.set(address, addressTxs);
                }
                let aTx = new AddressTransaction();
                aTx.coin = this.coin;
                aTx.balance_change = delta/1e8;
                aTx.height = block.height;
                aTx.tx_n = tx_n;
                aTx.timestamp = new Date(block.time*1000);
                let addressBalances = this.addressBalances.get(address);
                aTx.balance_after_block = addressBalances[addressBalances.length-1].balance;
                addressTxs.push(aTx);
            });
        }
    } 

    private async update(): Promise<void> {
        try {
            let height = await this.rpcClient.getBlockCount();
            if (this.height === undefined) {
                this.time = await this.getDbBlockTimestamp(height-BLOCKS_COUNT);
            }
            if (this.height == undefined || height > this.height) {
                let blockToDelete = this.blockByHeight.get(height-BLOCKS_COUNT);
                if (blockToDelete !== undefined) {
                    this.deleteBlock(blockToDelete);
                    this.deleteExpiredAddressTransactions(blockToDelete);
                }
                let newBlocks: MempoolBlock[] = [];
                let currentHeight;
                for (currentHeight = height; currentHeight > height-BLOCKS_COUNT; currentHeight--) {
                    let hash = await this.rpcClient.getBlockHash(currentHeight);
                    let oldBlock: MempoolBlock = this.blockByHeight.get(currentHeight);
                    if (oldBlock !== undefined) {
                        if (oldBlock.hash === hash) {
                            this.time = oldBlock.time;
                            this.height = oldBlock.height;
                            break;
                        } else {//Orphaned block
                            console.log("Orphaned "+this.coin.name+" block "+oldBlock.hash+" at height "+oldBlock.height);
                            this.deleteBlock(oldBlock);
                            this.deleteOrphanedAddressTransactions(oldBlock);
                            this.height = currentHeight-1;
                            this.time = this.blockByHeight.get(currentHeight-1).time;
                        }
                    }
                    let newBlock: RpcBlock = await this.rpcClient.getBlock(hash);
                    let mempoolBlock = new MempoolBlock(newBlock);
                    newBlocks.push(mempoolBlock);
                }
                newBlocks.reverse();
                let t: number = this.time;
                newBlocks.forEach(newBlock => {
                    if (t >= newBlock.time) {
                        t++;
                        newBlock.time = t;
                    } else {
                        t = newBlock.time;
                    }
                });
                let blocksTxDetails: TxDetails[][] = await this.blocksTxDetails(newBlocks);
                for (let i = 0; i < newBlocks.length; i++) {
                    let block = newBlocks[i];
                    let blockTxDetails: TxDetails[] = blocksTxDetails[i];
                    let blockAddressDeltas: Map<string, number> = new Map();
                    for (let tx_n = 0; tx_n < block.tx.length; tx_n++) {
                        let tx = block.tx[tx_n];
                        let txDetails = blockTxDetails[tx_n];
                        tx.fee = txDetails.fee;
                        txDetails.addressDeltas.forEach((delta: number, address: string) => {
                            let oldDelta = blockAddressDeltas.get(address);
                            blockAddressDeltas.set(address, oldDelta === undefined ? delta: oldDelta + delta);
                        });
                    }
                    //await this.updateAddressBalances(block, blockAddressDeltas);
                    let blockAddressBalances = await this.blockAddressBalances(block, blockAddressDeltas);
                    blockAddressBalances.forEach(e => {
                        let addressBalance: AddressBalance = new AddressBalance();
                        addressBalance.balance = e.balance;
                        addressBalance.timestamp = new Date(block.time*1000);
                        if (!this.addressBalances.has(e.address)) this.addressBalances.set(e.address, []);
                        this.addressBalances.get(e.address).push(addressBalance);
                    });
                    this.updateAddressTransactions(block, blockTxDetails, blockAddressDeltas);
                    this.blockByHeight.set(block.height, block);
                    this.blockByHash.set(block.hash, block);
                    this.height = block.height;
                    this.time = block.time;
                    console.log(this.coin.name+" height "+block.height);
                }
            }
        } catch(error) {
            throw error;
        }
    }
}

class TxDetails {
    public fee: number;
    public addressDeltas: Map<string, number>;
}