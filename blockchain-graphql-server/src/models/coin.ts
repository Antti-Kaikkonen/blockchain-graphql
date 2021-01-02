import { Field, ObjectType } from "type-graphql";
import { Mempool, MempoolBlock } from "../mempool";
import { Address } from "./address";
import { Date } from "./date";

@ObjectType()
export class Coin {
    @Field({nullable: false, complexity: 1})
    name: string;

    keyspace: string;

    mempool: Mempool;

    rpc_urls: string[];
    
}    