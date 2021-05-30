import { Field, ObjectType } from 'type-graphql'
import { Coin } from './coin'

@ObjectType('Mempool')
export class MempoolModel {

    @Field({ nullable: false, complexity: 1 })
    readonly coin: Coin

}