import { Resolver } from "type-graphql";
import { Richlist } from '../models/richlist';
import { Client, mapping } from "cassandra-driver";
import { Inject } from 'typedi';

@Resolver(of => Richlist)
export class RichlistResolver {

  constructor(@Inject("cassandra_client") private client: Client) {
  }

}