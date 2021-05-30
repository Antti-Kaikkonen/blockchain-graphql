import { Resolver } from 'type-graphql'
import { RichList } from '../models/richlist'
import { Inject } from 'typedi'
import { LimitedCapacityClient } from '../limited-capacity-client'

@Resolver(of => RichList)
export class RichlistResolver {

    constructor(@Inject('cassandra_client') private client: LimitedCapacityClient) {
    }

}