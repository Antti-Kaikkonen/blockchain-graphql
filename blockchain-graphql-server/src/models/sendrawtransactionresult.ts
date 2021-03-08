import { Field, ObjectType } from "type-graphql";

@ObjectType()
export class SendRawTransactionResult {

    @Field({ nullable: false, complexity: 1 })
    txid: string;

}