import { types } from "cassandra-driver"
import { Transform, TransformCallback } from "stream"
import { AddEvent2 } from "./block-fetcher"
import { DeleteEvent } from "./block-reader"
import { LimitedCapacityClient } from "../limited-capacity-client"
import { Coin } from "../models/coin"
import { RpcBlock, RpcClient, RpcTx, RpcVin } from "../rpc-client"
import { Mempool } from "./mempool"

export interface AddEvent3 extends AddEvent2 {
    inputDetails: Map<string, Promise<{ address: string, value: number }>>;
}

export class BlockInputDetailsFetcher extends Transform {

    public blockByHeight: Map<number, RpcBlock> = new Map()

    private processTx(tx: RpcTx, inputDetails: Map<string, Promise<{ address: string, value: number }>>) {
        const coinbase: boolean = tx.vin.length === 1 && tx.vin[0].coinbase !== undefined
        if (!coinbase) {
            tx.vin.forEach((vin, spending_index) => {
                const already_spent_in = this.outpointToInpoint.get(vin.txid + vin.vout)
                if (already_spent_in !== undefined && already_spent_in.spending_txid !== tx.txid) {
                    //console.log("Output " + vin.txid + "-" + vin.vout + " already spent by input " + already_spent_in.spending_txid + "-" + already_spent_in.spending_index + " but double spent by input " + tx.txid + "-" + spending_index);
                    this.txById.delete(already_spent_in.spending_txid)//Delete double spent transaction
                }
                this.outpointToInpoint.set(vin.txid + vin.vout, { spending_txid: tx.txid, spending_index: spending_index })
                inputDetails.set(vin.txid + vin.vout, this.getInputDetails(vin))
            })
        }
    }

    constructor(private client: LimitedCapacityClient, private rpcClient: RpcClient, private coin: Coin, private mempool: Mempool,
        public txById: Map<string, RpcTx>, public outpointToInpoint: Map<string, { spending_txid: string, spending_index: number }>) {
        super({
            objectMode: true,
            transform: async (event: DeleteEvent | AddEvent2, encoding: BufferEncoding, callback: TransformCallback) => {
                let blockToDelete: RpcBlock
                if (event.type === "add") {
                    const rpcBlock = await event.block
                    this.blockByHeight.set(rpcBlock.height, rpcBlock)
                    rpcBlock.tx.forEach(tx => {
                        this.txById.set(tx.txid, tx)
                    })
                    const inputDetails: Map<string, Promise<{ address: string, value: number }>> = new Map()
                    rpcBlock.tx.forEach(tx => {
                        this.processTx(tx, inputDetails)
                    })
                    this.push({ ...event, inputDetails: inputDetails })
                    blockToDelete = this.blockByHeight.get(event.height - 10)

                } else if (event.type === "delete") {
                    blockToDelete = this.blockByHeight.get(event.height)
                    this.push(event)
                }
                if (blockToDelete !== undefined) {
                    this.blockByHeight.delete(blockToDelete.height)
                    blockToDelete.tx.forEach(tx => {
                        this.txById.delete(tx.txid)
                        tx.vin.forEach(vin => {
                            this.outpointToInpoint.delete(vin.txid + vin.vout)
                        })
                    })
                }
                callback()
            }
        })
    }

    private async getInputDetailsFromDB(vin: RpcVin): Promise<{ address: string, value: number }> {
        const res: types.ResultSet = await this.client.execute("SELECT value, scriptpubkey.addresses FROM " + this.coin.keyspace + ".transaction_output WHERE txid = ? AND n=?;", [vin.txid, vin.vout], { prepare: true })
        if (res.rows.length === 0) {
            throw new Error(this.coin.name + " output " + vin.txid + "-" + vin.vout + " was not found in db. Make sure your db is synchronized with the blockchain.")
        } else {
            for (const row of res.rows) {
                const value: number = row.get("value")
                const addresses: string[] = row.get("scriptpubkey.addresses")
                let address: string
                if (addresses !== undefined && addresses !== null && addresses.length === 1) {
                    address = addresses[0]
                }
                return {
                    address: address,
                    value: value
                }
            }
        }
    }

    private getMempoolInputDetails(vin: RpcVin): { address: string, value: number } {
        const mempool_tx = this.txById.get(vin.txid)
        if (mempool_tx === undefined) {
            return undefined
        } else {
            let address: string
            const spent_output = mempool_tx.vout[vin.vout]
            if (spent_output.scriptPubKey.addresses !== undefined && spent_output.scriptPubKey.addresses !== null && spent_output.scriptPubKey.addresses.length === 1) {
                address = spent_output.scriptPubKey.addresses[0]
            }
            return {
                address: address,
                value: spent_output.value
            }
        }
    }

    private getInputDetails(vin: RpcVin): Promise<{ address: string, value: number }> {
        const mempoolDetails = this.getMempoolInputDetails(vin)
        if (mempoolDetails !== undefined) {
            return Promise.resolve(mempoolDetails)
        } else {
            return this.getInputDetailsFromDB(vin)
        }
    }

}