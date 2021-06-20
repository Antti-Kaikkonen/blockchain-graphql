import { Resolver } from 'type-graphql'
import { Inject, Service } from 'typedi'
import { OHLC } from '../models/ohlc'
import { LimitedCapacityClient } from '../limited-capacity-client'

@Service()
@Resolver(of => OHLC)
export class OHLCResolver {

    constructor(@Inject('cassandra_client') private client: LimitedCapacityClient) {
    }

}