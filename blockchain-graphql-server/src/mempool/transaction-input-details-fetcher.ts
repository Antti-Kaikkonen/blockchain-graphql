import { types } from "cassandra-driver";
import { Transform, TransformCallback } from "stream";
import { MempoolEvent2 } from "./block-fetcher";
import { ResolvedMempoolTransaction } from "./unconfirmed-transaction-waiter";
import { LimitedCapacityClient } from "../limited-capacity-client";
import { Coin } from "../models/coin";
import { RpcClient, RpcTx, RpcVin } from "../rpc-client";
import { Mempool } from "./mempool";

export interface MempoolEvent3 extends MempoolEvent2 {
    inputDetails: Map<string, Promise<{ address: string, value: number }>>;
}


export class TransactionInputDetailsFetcher extends Transform {

    private processTx(tx: RpcTx, inputDetails: Map<string, Promise<{ address: string, value: number }>>) {
        const coinbase: boolean = tx.vin.length === 1 && tx.vin[0].coinbase !== undefined;
        if (!coinbase) {
            tx.vin.forEach((vin, spending_index) => {
                const already_spent_in = this.outpointToInpoint.get(vin.txid + vin.vout);
                if (already_spent_in !== undefined && already_spent_in.spending_txid !== tx.txid) {
                    //console.log("Output " + vin.txid + "-" + vin.vout + " already spent by input " + already_spent_in.spending_txid + "-" + already_spent_in.spending_index + " but double spent by input " + tx.txid + "-" + spending_index);
                    this.txById.delete(already_spent_in.spending_txid);//Delete double spent transaction
                }
                this.outpointToInpoint.set(vin.txid + vin.vout, { spending_txid: tx.txid, spending_index: spending_index });
                inputDetails.set(vin.txid + vin.vout, this.getInputDetails(vin));
            });
        }
    }

    constructor(private client: LimitedCapacityClient, private rpcClient: RpcClient, private coin: Coin, private mempool: Mempool,
        public txById: Map<string, RpcTx>, public outpointToInpoint: Map<string, { spending_txid: string, spending_index: number }>) {
        super({
            objectMode: true,
            transform: async (event: ResolvedMempoolTransaction, encoding: BufferEncoding, callback: TransformCallback) => {
                if (event.type === "hashtx") {
                    if (!this.txById.has(event.txid)) {
                        this.txById.set(event.txid, event.rpcTx);
                        const inputDetails: Map<string, Promise<{ address: string, value: number }>> = new Map();
                        this.processTx(event.rpcTx, inputDetails);
                        this.push({ ...event, inputDetails: inputDetails });
                    }
                }
                callback();
            }
        })
    }

    private async getInputDetailsFromDB(vin: RpcVin): Promise<{ address: string, value: number }> {
        const res: types.ResultSet = await this.client.execute("SELECT value, scriptpubkey.addresses FROM " + this.coin.keyspace + ".transaction_output WHERE txid = ? AND n=?;", [vin.txid, vin.vout], { prepare: true });
        if (res.rows.length === 0) {
            throw new Error(this.coin.name + " output " + vin.txid + "-" + vin.vout + " was not found in db. Make sure your db is synchronized with the blockchain.");
        } else {
            for (const row of res.rows) {
                const value: number = row.get("value");
                const addresses: string[] = row.get("scriptpubkey.addresses");
                let address: string;
                if (addresses !== undefined && addresses !== null && addresses.length === 1) {
                    address = addresses[0];
                }
                return {
                    address: address,
                    value: value
                };
            }
        }
    }

    private getMempoolInputDetails(vin: RpcVin): { address: string, value: number } {
        const mempool_tx = this.txById.get(vin.txid);
        if (mempool_tx === undefined) {
            return undefined;
        } else {
            let address: string;
            const spent_output = mempool_tx.vout[vin.vout];
            if (spent_output.scriptPubKey.addresses !== undefined && spent_output.scriptPubKey.addresses !== null && spent_output.scriptPubKey.addresses.length === 1) {
                address = spent_output.scriptPubKey.addresses[0];
            }
            return {
                address: address,
                value: spent_output.value
            };
        }
    }

    private async getInputDetails(vin: RpcVin): Promise<{ address: string, value: number }> {
        let fails = 0;
        while (true) {
            try {
                const mempoolDetails = this.getMempoolInputDetails(vin);
                if (mempoolDetails !== undefined) {
                    return mempoolDetails;
                } else {
                    return await this.getInputDetailsFromDB(vin);
                }
            } catch (err) {
                if (++fails > 100) {
                    throw err;
                }
                await new Promise((resolve, reject) => {
                    setTimeout(() => {
                        resolve(null);
                    }, 100);
                });
            }

        }
    }

}