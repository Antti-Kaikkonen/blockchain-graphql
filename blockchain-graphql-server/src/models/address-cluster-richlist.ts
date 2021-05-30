import { ObjectType, Field, InputType } from 'type-graphql'
import { AddressCluster } from './address-cluster'
import { PaginatedResponse } from './paginated-response'

@ObjectType()
export class AddressClusterRichlist {

    @Field({ nullable: false, complexity: 1 })
    cluster: AddressCluster

    @Field({ nullable: false, complexity: 1 })
    balance: number

}

@InputType()
export class AddressClusterRichlistCursor {

    @Field({ nullable: false })
    balance: number

    @Field({ nullable: false })
    clusterId: string

}

@ObjectType()
export class PaginatedAddressClusterRichlistResponse extends PaginatedResponse(AddressClusterRichlist) {
    @Field({ nullable: false, complexity: 0 })
    hasMore: boolean
}