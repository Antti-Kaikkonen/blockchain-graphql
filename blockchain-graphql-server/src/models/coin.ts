import { Field, Int, ObjectType } from "type-graphql";
import { Mempool } from "../mempool/mempool";

@ObjectType()
export class Coin {
    @Field({ nullable: false, complexity: 1 })
    name: string;

    @Field(type => Int, { nullable: false, complexity: 1 })
    bip44_index: number;

    @Field({ nullable: false, complexity: 1 })
    bip44_symbol: string;

    keyspace: string;

    mempool: Mempool;

    rpcUrls: string[];

    zmq_addresses: string[];

}