import { Transform, TransformCallback } from "stream";
import { AddEvent, ChainEvent, MempoolEvent } from "./block-reader";
import { RpcBlock, RpcClient, RpcTx } from "../rpc-client";
import { Mempool } from "./mempool";

export interface AddEvent2 extends AddEvent {
    block: Promise<RpcBlock>
}

export interface MempoolEvent2 extends MempoolEvent {
    rpcTx: Promise<RpcTx>;
}


export class BlockFetcher extends Transform {


    constructor(private rpcClient: RpcClient, private mempool: Mempool) {
        super({
            objectMode: true,
            transform: (event: ChainEvent, encoding: BufferEncoding, callback: TransformCallback) => {
                if (event.type === "add") {
                    this.push(<AddEvent2>{ ...event, block: this.rpcClient.getBlock(event.hash) });
                } else if (event.type === "delete") {
                    this.push(event);
                }
                callback();
            }
        })
    }

}