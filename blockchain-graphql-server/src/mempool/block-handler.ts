import { Writable } from "stream";
import { AddEvent3, MempoolEvent3 } from "./block-input-details-fetcher";
import { DeleteEvent } from "./block-reader";
import { LimitedCapacityClient } from "../limited-capacity-client";
import { MempoolBlock, MempoolTx } from "../mempool/mempool";
import { AddressBalance } from "../models/address-balance";
import { AddressTransaction } from "../models/address-transaction";
import { Coin } from "../models/coin";
import { Mempool } from "./mempool";
import { AddressUnconfirmedMempool } from './unconfirmed_mempool';

export interface TxDetails {
    fee: number;
    addressDeltas: Map<string, number>;
}

export class BlockHandler extends Writable {

    private processTx(tx: MempoolTx) {
        this.mempool.txById.set(tx.rpcTx.txid, tx);
        let coinbase: boolean = tx.rpcTx.vin.length === 1 && tx.rpcTx.vin[0].coinbase !== undefined;
        if (!coinbase) {
            tx.rpcTx.vin.forEach((vin, spending_index) => {
                const already_spent_in = this.mempool.outpointToInpoint.get(vin.txid+vin.vout);
                if (already_spent_in !== undefined && already_spent_in.spending_txid !== tx.rpcTx.txid) {
                    this.mempool.txById.delete(already_spent_in.spending_txid);//Delete double spent transaction
                    this.mempool.unconfirmedMempool.remove(already_spent_in.spending_txid);
                }
                this.mempool.outpointToInpoint.set(vin.txid+vin.vout, {spending_txid: tx.rpcTx.txid, spending_index: spending_index});
            });
        }
    }

    constructor(private client: LimitedCapacityClient, private coin: Coin, private mempool: Mempool) {
        super({
            objectMode: true, 
            write: async (event: DeleteEvent | AddEvent3 | MempoolEvent3, encoding: BufferEncoding, callback: (error?: any) => void) => {
                let blockToDelete: MempoolBlock;
                if (event.type === "hashtx") {
                    let rpcTx = await event.rpcTx;
                    if (!this.mempool.txById.has(rpcTx.txid)) {
                        let tx = new MempoolTx(rpcTx);
                        this.processTx(tx);
                        let txDetails = await this.txDetails(tx, event.inputDetails);
                        tx.fee = txDetails.fee;
                        this.mempool.unconfirmedMempool.add(tx, txDetails);
                        this.mempool.txById.set(tx.rpcTx.txid, tx);
                    }
                } else if (event.type === "add") {
                    if (this.mempool.time === undefined) {
                        this.mempool.time = await this.getDbBlockTimestamp(event.height-1);
                    }
                    this.mempool.height = event.height;
                    let mempoolBlock = new MempoolBlock(await event.block);
                    if (mempoolBlock.rpcBlock.time <= this.mempool.time) {
                        mempoolBlock.rpcBlock.time = ++this.mempool.time;//Make block timestamps stricly increasing
                    } else {
                        this.mempool.time = mempoolBlock.rpcBlock.time;
                    }
                    this.mempool.blockByHash.set(mempoolBlock.rpcBlock.hash, mempoolBlock);
                    this.mempool.blockByHeight.set(mempoolBlock.rpcBlock.height, mempoolBlock);
                    mempoolBlock.tx.forEach(tx => {
                        this.mempool.unconfirmedMempool.remove(tx.rpcTx.txid);
                        this.processTx(tx);
                    });
                    let inputDetails: Map<string, Promise<{address: string, value: number}>> = event.inputDetails;
                    let blockAddressDeltas: Map<string, number> = new Map();
                    let blockTxDetails = await Promise.all(mempoolBlock.tx.map(tx => this.txDetails(tx, inputDetails)));
                    for (let i = 0; i < mempoolBlock.tx.length; i++) {
                        let txDetails = blockTxDetails[i];
                        let tx = mempoolBlock.tx[i];
                        tx.fee = txDetails.fee;
                        txDetails.addressDeltas.forEach((delta: number, address: string) => {
                            let oldDelta = blockAddressDeltas.get(address);
                            blockAddressDeltas.set(address, oldDelta === undefined ? delta: oldDelta + delta);
                        });
                    }
                    let blockAddressBalances = await this.blockAddressBalances(mempoolBlock, blockAddressDeltas);
                    blockAddressBalances.forEach(e => {
                        let addressBalance: AddressBalance = new AddressBalance();
                        addressBalance.balance = e.balance;
                        addressBalance.timestamp = new Date(mempoolBlock.rpcBlock.time*1000);
                        if (!this.mempool.addressBalances.has(e.address)) this.mempool.addressBalances.set(e.address, []);
                        this.mempool.addressBalances.get(e.address).push(addressBalance);
                    });
                    this.updateAddressTransactions(mempoolBlock, blockTxDetails, blockAddressDeltas);
                    blockToDelete = this.mempool.blockByHeight.get(event.height-10);
                    if (blockToDelete !== undefined) this.deleteExpiredAddressTransactions(blockToDelete);
                } else if (event.type === "delete") {
                    this.mempool.height = event.height-1;
                    blockToDelete = this.mempool.blockByHash.get(event.hash);
                    this.deleteOrphanedAddressTransactions(blockToDelete);
                }
                if (blockToDelete !== undefined) {
                    blockToDelete.tx.forEach(tx => {
                        this.mempool.txById.delete(tx.rpcTx.txid);
                        tx.rpcTx.vin.forEach(vin => {
                            this.mempool.outpointToInpoint.delete(vin.txid+vin.vout);
                        });
                    });
                    this.mempool.blockByHeight.delete(blockToDelete.rpcBlock.height);
                    this.mempool.blockByHash.delete(blockToDelete.rpcBlock.hash);
                }
                callback(null);
            }
        });
    }


    private async txDetails(tx: MempoolTx, inputToDetails: Map<string, Promise<{address: string, value: number}>>): Promise<TxDetails> {
        return new Promise(async (resolve, reject) => {
            let feeSats: number = 0;
            let addressDeltas: Map<string, number> = new Map();
            let coinbase: boolean = tx.rpcTx.vin.length === 1 && tx.rpcTx.vin[0].coinbase !== undefined;
            tx.rpcTx.vout.forEach(vout => {
                let valueSats = Math.round(vout.value*1e8);
                if (vout.scriptPubKey.addresses !== undefined && vout.scriptPubKey.addresses !== null && vout.scriptPubKey.addresses.length === 1) {
                    let address: string = vout.scriptPubKey.addresses[0];
                    let oldValue = addressDeltas.get(address);
                    addressDeltas.set(address, oldValue === undefined ? valueSats : oldValue + valueSats);
                }
                if (!coinbase) feeSats -= valueSats;
            });
            if (!coinbase && tx.rpcTx.vin.length > 0) {
                let pending_promises = tx.rpcTx.vin.length;
                tx.rpcTx.vin.forEach(async (vin, spending_index) => {
                    let inputDetails = await inputToDetails.get(vin.txid+vin.vout);
                    let valueSats = Math.round(inputDetails.value*1e8);
                    feeSats+=valueSats;
                    if (inputDetails.address !== undefined) {
                        let oldValue = addressDeltas.get(inputDetails.address);
                        addressDeltas.set(inputDetails.address, oldValue === undefined ? -valueSats : oldValue - valueSats);
                    }
                    pending_promises--;
                    if (pending_promises === 0) {
                        resolve({
                            fee: feeSats/1e8,
                            addressDeltas: addressDeltas
                        });
                    }
                });
            } else {
                resolve({
                    fee: feeSats/1e8,
                    addressDeltas: addressDeltas
                });
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

    private async getDbAddressBalance(address: string, beforeTimestamnp: number): Promise<number> {
        try {
            let res = await this.client.execute("SELECT balance FROM "+this.coin.keyspace+".address_balance WHERE address = ? AND timestamp < ? LIMIT 1;", [address, beforeTimestamnp], {prepare: true});
            let balance: number;
            res.rows.forEach(row => {
                balance = row.get("balance");
            });
            return balance;
        } catch(error) {
            throw error;
        }
    }

    private async blockAddressBalances(block: MempoolBlock, blockAddressDeltas: Map<string, number>): Promise<{address: string, balance: number}[]> {
        return Promise.all(Array.from(blockAddressDeltas.entries()).map(async ([address, delta]) => {
            try {
                let oldBalances = this.mempool.addressBalances.get(address);
                let oldBalance: number;
                if (oldBalances === undefined) {
                    //this.addressBalances.set(address, []);
                    oldBalance = await this.getDbAddressBalance(address, block.rpcBlock.time*1000);
                    if (oldBalance === undefined) {
                        oldBalance = 0;
                    } else {
                        oldBalance = Math.round(oldBalance*1e8);
                    }
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
                let addressTxs = this.mempool.addressTransactions.get(address);
                if (addressTxs === undefined) {
                    addressTxs = [];
                    this.mempool.addressTransactions.set(address, addressTxs);
                }
                let aTx = new AddressTransaction();
                aTx.coin = this.coin;
                aTx.balanceChange = delta/1e8;
                aTx.height = block.rpcBlock.height;
                aTx.txN = tx_n;
                aTx.timestamp = new Date(block.rpcBlock.time*1000);
                let addressBalances = this.mempool.addressBalances.get(address);
                aTx.balanceAfterBlock = addressBalances[addressBalances.length-1].balance;
                addressTxs.push(aTx);
            });
        }
    } 
    private deleteOrphanedAddressTransactions(block: MempoolBlock): void {
        this.mempool.addressBalances.forEach((balances: AddressBalance[], address: string) => {
            while (balances.length > 0 && balances[balances.length-1].timestamp.getTime() === block.rpcBlock.time*1000) {
                balances.pop();
            }
            if (balances.length === 0) this.mempool.addressBalances.delete(address);
        });
        this.mempool.addressTransactions.forEach((txs: AddressTransaction[], address: string) => {
            while (txs.length > 0 && txs[txs.length-1].timestamp.getTime() === block.rpcBlock.time*1000) {
                txs.pop();
            }
            if (txs.length === 0) this.mempool.addressTransactions.delete(address);
        });
    }

    private deleteExpiredAddressTransactions(block: MempoolBlock): void {
        this.mempool.addressBalances.forEach((balances: AddressBalance[], address: string) => {
            while (balances.length > 0 && balances[0].timestamp.getTime() === block.rpcBlock.time*1000) {
                balances.shift();
            }
            if (balances.length === 0) this.mempool.addressBalances.delete(address);
        });
        this.mempool.addressTransactions.forEach((txs: AddressTransaction[], address: string) => {
            while (txs.length > 0 && txs[0].timestamp.getTime() === block.rpcBlock.time*1000) {
                txs.shift();
            }
            if (txs.length === 0) this.mempool.addressTransactions.delete(address);
        });
    }
}