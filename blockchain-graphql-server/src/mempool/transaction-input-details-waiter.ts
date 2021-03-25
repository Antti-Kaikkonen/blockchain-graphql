import { Transform, TransformCallback } from "stream";
import { RpcTx } from "../rpc-client";
import { MempoolEvent3 } from "./transaction-input-details-fetcher";
import { MempoolEvent } from "./block-reader";

export interface ResolvedMempoolTransaction extends MempoolEvent {
    rpcTx: RpcTx;
}

export class TransactionInputDetailsWaiter extends Transform {
    constructor() {
        super({
            objectMode: true,
            transform: async (event: MempoolEvent3, encoding: BufferEncoding, callback: TransformCallback) => {
                for (const [, v] of event.inputDetails) {
                    await v;
                }
                this.push(event);
                callback();
            }
        });
    }
}