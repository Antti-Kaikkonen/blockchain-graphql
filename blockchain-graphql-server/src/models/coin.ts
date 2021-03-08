import { Field, ObjectType } from "type-graphql";
import { Mempool } from "../mempool/mempool";

@ObjectType()
export class Coin {
    @Field({ nullable: false, complexity: 1 })
    name: string;

    keyspace: string;

    mempool: Mempool;

    rpcUrls: string[];

    zmq_addresses: string[];

}