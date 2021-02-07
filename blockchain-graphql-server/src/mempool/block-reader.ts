import { Readable } from "stream";
import { Coin } from "../models/coin";
import { RpcClient } from "../rpc-client";
import { Mempool } from "./mempool";

export interface MempoolEvent {
    type: "hashtx"
    txid: string
}

export interface ChainEvent {
    type: "add" | "delete";
    hash: string;
    height: number;
}

export interface AddEvent extends ChainEvent{
    type: "add";
}

export interface DeleteEvent extends ChainEvent{
    type: "delete";
}


export class BlockReader extends Readable {

    private currentHeight: number;

    constructor(private rpcClient: RpcClient, private coin: Coin, private maxReorgDepth: number, private mempool: Mempool) {
        super({objectMode: true});
        console.log("NEW BLOCKREADER");
    }

    private heightToHash: Map<number, string> = new Map();

    private async newBlocksToHeight(height: number) {
        let hash = await this.rpcClient.getBlockHash(height);
        let oldHash: string = this.heightToHash.get(height);
        this.heightToHash.set(height, hash);
        if (oldHash !== undefined) {
            if (oldHash === hash) {//Found common ancestor
                return;
            } else {
                this.push(<ChainEvent>{type: "delete", hash: hash, height: height});
                await this.newBlocksToHeight(height-1);//Orphaned
            }
        } else if (!this.heightToHash.has(height+this.maxReorgDepth)) {
            await this.newBlocksToHeight(height-1);
        } else if (this.currentHeight !== undefined) {
            throw new Error("Deep blockchain reorganization detected (hash: "+hash+", height: "+height+", old chain tip height: "+this.currentHeight+")");
        }
        this.push(<ChainEvent>{type: "add", hash: hash, height: height});
    }

    async _read(n: number) {
        let height: number;
        while(true) {
            if (this.destroyed) return;
            height = await this.rpcClient.getBlockCount();
            if (this.currentHeight === undefined || height > this.currentHeight) {
                console.log(this.coin.name+"\t"+height);
                break;
            }
            await new Promise((resolve) => {
                setTimeout(resolve, 100);
            });
        }
        await this.newBlocksToHeight(height);
        this.currentHeight = height;
    }
    
}