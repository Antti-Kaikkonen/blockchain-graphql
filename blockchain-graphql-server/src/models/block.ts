import { ObjectType, Field, Int } from "type-graphql"
import { Coin } from "./coin"

@ObjectType()
export class Block {

    @Field({ nullable: false, complexity: 1 })
    hash: string

    @Field(type => Int, { nullable: false, complexity: 1 })
    height: number

    @Field(type => Int, { nullable: false, complexity: 1 })
    size: number

    @Field(type => Int, { nullable: true, complexity: 1 })
    version: number

    @Field({ nullable: false, complexity: 1 })
    versionHex: string

    @Field({ nullable: false, complexity: 1 })
    merkleRoot: string

    @Field({ nullable: false, complexity: 1 })
    time: Date

    @Field(type => Int, { nullable: false, complexity: 1 })
    medianTime: number

    @Field(type => Int, { nullable: false, complexity: 1 })
    nonce: number

    @Field({ nullable: false, complexity: 1 })
    bits: string

    @Field({ nullable: false, complexity: 1 })
    difficulty: number

    @Field({ nullable: false, complexity: 1 })
    chainWork: string

    @Field({ nullable: false, complexity: 1 })
    previousBlockHash: string

    @Field(type => Int, { nullable: false, complexity: 1 })
    txCount: number

    @Field({ nullable: false, complexity: 1 })
    readonly coin: Coin

}