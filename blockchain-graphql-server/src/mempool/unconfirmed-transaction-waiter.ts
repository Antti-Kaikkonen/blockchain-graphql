import { Transform, TransformCallback } from "stream";
import { RpcTx } from "../rpc-client";
import { MempoolEvent2 } from "./block-fetcher";
import { MempoolEvent } from "./block-reader";

export interface ResolvedMempoolTransaction extends MempoolEvent {
    rpcTx: RpcTx;
}

export class UnconfirmedTransactionWaiter extends Transform {
    constructor() {
        super({
            objectMode: true,
            transform: async (event: MempoolEvent2, encoding: BufferEncoding, callback: TransformCallback) => {
                let rpcTx = await event.rpcTx;
                if (rpcTx !== undefined && rpcTx !== null) {
                    this.push(<ResolvedMempoolTransaction>{ txid: event.txid, rpcTx: rpcTx, type: "hashtx" });
                }
                callback();
            }
        });
    }
}