import { ObjectType, Field } from "type-graphql";
import { TransactionOutput } from "./transaction-output";

@ObjectType()
export class ScriptSig {
    @Field({ nullable: false, complexity: 1 })
    asm: string;
    @Field({ nullable: false, complexity: 1 })
    hex: string;
}