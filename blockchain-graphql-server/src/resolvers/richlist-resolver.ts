import { Resolver } from "type-graphql";
import { Richlist } from '../models/richlist';
import { Inject } from 'typedi';
import { LimitedCapacityClient } from "../limited-capacity-client";

@Resolver(of => Richlist)
export class RichlistResolver {

  constructor(@Inject("cassandra_client") private client: LimitedCapacityClient) {
  }

}