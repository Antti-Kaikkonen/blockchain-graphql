import { Resolver, Query, Arg, FieldResolver, Root, ArgsDictionary, Subscription, Args, PubSub, PubSubEngine } from "type-graphql";
import { Client, types } from "cassandra-driver";
import { Inject } from 'typedi';
import { Address } from "../models/address";
import { AddressTransaction, AddressTransactionCursor, PaginatedAddressTransactionResponse } from "../models/address-transaction";
import { OHLCCursor, OHLC, PaginatedOHLCResponse } from '../models/ohlc';
import { AddressBalanceCursor, PaginatedAddressBalanceResponse, AddressBalance } from "../models/address-balance";
import { AddressCluster } from "../models/address-cluster";


@Resolver(of => Address)
export class AddressResolver {

  constructor(@Inject("cassandra_client") private client: Client) {
  }
  

  @Query(returns => Address, {complexity: 1})
  async address(@Arg("address") address: string) {
    let res = new Address(address);
    return res;
  }

  @FieldResolver(returns => AddressCluster, {complexity: ({ childComplexity, args }) => 100 + childComplexity})
  async guestimatedWallet(@Root() address: Address): Promise<AddressCluster> {
    let query: string = "SELECT parent FROM union_find WHERE address=?";
    let currentAddress = address.address;
    do {
      let resultSet: types.ResultSet = await this.client.execute(
        query, 
        [currentAddress], 
        {prepare: true}
      );
      if (resultSet.rows.length === 1) {
        let parent: string = resultSet.rows[0].get("parent");
        if (parent !== null && parent !== undefined) {
          currentAddress = parent;
        } else {
          let res: AddressCluster = new AddressCluster();
          res.clusterId = currentAddress;
          return res;
        }
      } else {
        return null;
      }
    } while(true);
  }


  @FieldResolver({complexity: ({ childComplexity, args }) => 100 + args.limit * childComplexity})
  async ohlc(@Root() address: Address, 
    @Arg("cursor", {nullable: true}) cursor: OHLCCursor,
    @Arg("interval", {nullable: true, defaultValue: 1000*60*60*24}) interval?: number,
    @Arg("limit", {nullable: true, defaultValue: 1000}) limit?: number,
    //@Arg("reverse", {nullable: true, defaultValue: false}) reverse?: boolean,
  ): Promise<PaginatedOHLCResponse> {
    let reverse: boolean = false;
    console.log("interval = "+interval);
    let args: any[] = [address.address, interval];
    let query: string = "SELECT timestamp, open, high, low, close FROM ohlc WHERE address=? AND interval=?";
    if (cursor) {
      query += " AND timestamp " + (reverse ? "<" : ">") + " ?";
      args = args.concat([cursor.timestamp]);
    }
    if (reverse) query += " ORDER BY timestamp DESC";
    console.log("query: "+query+", args: "+args);
    let resultSet: types.ResultSet = await this.client.execute(
      query, 
      args, 
      {prepare: true, fetchSize: limit}
    );
    let res:  OHLC[] = resultSet.rows.map(row => {
      let ohlc = new OHLC();
      ohlc.timestamp = row.get("timestamp");
      ohlc.open = row.get("open")/1e8;
      ohlc.high = row.get("high")/1e8;
      ohlc.low = row.get("low")/1e8;
      ohlc.close = row.get("close")/1e8;
      return ohlc;
    });
    return {
      hasMore: resultSet.pageState !== null,
      items: res,
    }
  }

  @FieldResolver({complexity: ({ childComplexity, args }) => 100 + args.limit * childComplexity})
  async addressTransactions(@Root() address: Address, 
    @Arg("cursor", {nullable: true}) cursor: AddressTransactionCursor,
    @Arg("limit", {nullable: true, defaultValue: 1000}) limit?: number, 
  ): Promise<PaginatedAddressTransactionResponse> {
    let args: any[] = [address.address];
    let query: string = "SELECT timestamp, height, tx_n, balance_change FROM address_transaction WHERE address=?";
    if (cursor) {
      query += " AND (timestamp, height, tx_n) < (?, ?, ?)";
      args = args.concat([cursor.timestamp, cursor.height, cursor.tx_n]);
    }
    let resultSet: types.ResultSet = await this.client.execute(
      query, 
      args, 
      {prepare: true, fetchSize: limit}
    );
    let res: AddressTransaction[] = resultSet.rows.map(row => {
      let addressTransaction = new AddressTransaction();
      addressTransaction.timestamp = row.get("timestamp");
      addressTransaction.height = row.get("height");
      addressTransaction.tx_n = row.get("tx_n");
      addressTransaction.balance_change = row.get("balance_change")/1e8;
      return addressTransaction;
    });
    if (res.length > 0) {
      let start = res[res.length-1].timestamp;
      let end = res[0].timestamp;
      let query2: string = "SELECT timestamp, balance FROM address_balance WHERE address=? AND timestamp >= ? AND timestamp <= ?";
      let args2: any[] = [address.address, start, end];
      let resultSet2: types.ResultSet = await this.client.execute(
        query2, 
        args2, 
        {prepare: true}
      );
      let time2Balance: Map<number, number> = new Map();
      resultSet2.rows.forEach(row => time2Balance.set(row.get("timestamp").getTime(), row.get("balance")/1e8));
      res.forEach(r => r.balance_after_block = time2Balance.get(r.timestamp.getTime()));
    }
    return {
      hasMore: resultSet.pageState !== null,
      items: res,
    };
  }

  @FieldResolver({complexity: ({ childComplexity, args }) => 100 + args.limit * childComplexity})
  async addressBalances(@Root() address: Address, 
    @Arg("cursor", {nullable: true}) cursor: AddressBalanceCursor,
    @Arg("limit", {nullable: true, defaultValue: 1000}) limit?: number,
    //@Arg("reverse", {nullable: true, defaultValue: false}) reverse?: boolean,
  ): Promise<PaginatedAddressBalanceResponse> {
    let args: any[] = [address.address];
    let query: string = "SELECT timestamp, balance FROM address_balance WHERE address=?";
    if (cursor) {
      query += " AND timestamp < ?";
      args = args.concat([cursor.timestamp]);
    }
    console.log("query: "+query+", args: "+args);
    let resultSet: types.ResultSet = await this.client.execute(
      query, 
      args, 
      {prepare: true, fetchSize: limit}
    );
    let res:  AddressBalance[] = resultSet.rows.map(row => {
      let addressBalance = new AddressBalance();
      addressBalance.timestamp = row.get("timestamp");
      addressBalance.balance = row.get("balance")/1e8;
      return addressBalance;
    });
    return {
      hasMore: resultSet.pageState !== null,
      items: res,
    }
  };

  //TODO @FieldResolver() addressTransactions https://typegraphql.com/docs/resolvers.html#field-resolvers


}