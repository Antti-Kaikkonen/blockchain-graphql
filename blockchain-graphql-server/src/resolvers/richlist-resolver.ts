import { Resolver } from 'type-graphql'
import { RichList } from '../models/richlist'
import { Inject, Service } from 'typedi'
import { LimitedCapacityClient } from '../limited-capacity-client'

@Service()
@Resolver(of => RichList)
export class RichlistResolver {

    constructor(@Inject('cassandra_client') private client: LimitedCapacityClient) {
    }

}