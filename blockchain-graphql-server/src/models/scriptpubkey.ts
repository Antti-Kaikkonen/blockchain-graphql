import { ObjectType, Field, Int } from "type-graphql";
import { Address } from "./address";

@ObjectType()
export class ScriptPubKey {
    @Field({nullable: false, complexity: 1})
    asm: string;
    @Field({nullable: false, complexity: 1})
    hex: string;
    @Field(type => Int, {nullable: false, complexity: 1})
    reqSigs: number;
    @Field({nullable: false, complexity: 1})
    type: string;
    @Field(type => [Address], {nullable: true, complexity: 1})
    addresses: Address[];

}