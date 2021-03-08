import { Transform, TransformCallback } from "stream";
import { RpcClient, RpcTx } from "../rpc-client";
import { MempoolEvent2 } from "./block-fetcher";
import { MempoolEvent } from "./block-reader";
import { Mempool } from "./mempool";

export class UnconfirmedTransactionFetcher extends Transform {

    private async getTransaction(txid: string): Promise<RpcTx> {
        return new Promise(async (resolve, reject) => {
            let fails = 0;
            while (true) {
                try {
                    resolve(await this.rpcClient.getRawTransaction(txid));
                    break;
                } catch (err) {
                    if (++fails > 100) {
                        console.log("Can't find " + txid);
                        resolve(null);
                        break;
                    }
                    if (this.mempool.txById.has(txid)) {
                        resolve(null);
                        break;
                    }
                    await new Promise((resolve, reject) => {
                        setTimeout(() => {
                            resolve(null);
                        }, 100);
                    });
                }
            }
        });
    }

    constructor(private rpcClient: RpcClient, private mempool: Mempool) {
        super({
            objectMode: true,
            transform: (event: MempoolEvent, encoding: BufferEncoding, callback: TransformCallback) => {
                if (!this.mempool.txById.has(event.txid)) {
                    this.push(<MempoolEvent2>{ type: "hashtx", txid: event.txid, rpcTx: this.getTransaction(event.txid) });
                }
                callback();
            }
        });
    }
}