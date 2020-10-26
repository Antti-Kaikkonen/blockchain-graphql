import { Resolver } from "type-graphql";
import { Client, types, mapping } from "cassandra-driver";
import { Inject } from 'typedi';
import { OHLC } from "../models/ohlc";

@Resolver(of => OHLC)
export class OHLCResolver {

  constructor(@Inject("cassandra_client") private client: Client) {
  }

}