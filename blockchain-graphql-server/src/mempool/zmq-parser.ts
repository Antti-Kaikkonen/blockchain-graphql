import { Transform, TransformCallback } from "stream";
import { MempoolEvent } from "./block-reader";

export class ZmqParser extends Transform {
    constructor() {
        super({
            objectMode: true,
            transform: ([topic, msg]: [Buffer, Buffer], encoding: BufferEncoding, callback: TransformCallback) => {
                let txid = msg.toString("hex");
                callback(null, <MempoolEvent>{
                    type: "hashtx",
                    txid: txid
                });
            }
        })
    }
}