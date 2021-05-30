import { ObjectType, Field } from 'type-graphql'

@ObjectType()
export class ScriptSig {
    @Field({ nullable: false, complexity: 1 })
    asm: string
    @Field({ nullable: false, complexity: 1 })
    hex: string
}