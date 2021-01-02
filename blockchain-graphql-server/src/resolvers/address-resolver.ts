import { Resolver, Arg, FieldResolver, Root } from "type-graphql";
import { types } from "cassandra-driver";
import { Inject } from 'typedi';
import { Address } from "../models/address";
import { AddressTransaction, AddressTransactionCursor, PaginatedAddressTransactionResponse } from "../models/address-transaction";
import { OHLCCursor, OHLC, PaginatedOHLCResponse } from '../models/ohlc';
import { AddressBalanceCursor, PaginatedAddressBalanceResponse, AddressBalance } from "../models/address-balance";
import { AddressCluster } from "../models/address-cluster";
import { LimitedCapacityClient } from "../limited-capacity-client";


@Resolver(of => Address)
export class AddressResolver {

  constructor(
    @Inject("cassandra_client") private client: LimitedCapacityClient,
  ) {}


  @FieldResolver(returns => AddressCluster, {complexity: ({ childComplexity, args }) => 100 + childComplexity})
  async guestimatedWallet(@Root() address: Address): Promise<AddressCluster> {
    let query: string = "SELECT parent FROM "+address.coin.keyspace+".union_find WHERE address=?";
    let currentAddress = address.address;
    do {
      let resultSet: types.ResultSet = await this.client.execute(
        query, 
        [currentAddress], 
        {prepare: true}
      );
      if (resultSet.rows.length === 1) {
        currentAddress = resultSet.rows[0].get("parent");
      } else {
        let res: AddressCluster = new AddressCluster();
        res.clusterId = currentAddress;
        res.coin = address.coin;
        return res;
      }
    } while(true);
  }


  @FieldResolver({complexity: ({ childComplexity, args }) => 100 + args.limit * childComplexity})
  async ohlc(@Root() address: Address, 
    @Arg("cursor", {nullable: true}) cursor: OHLCCursor,
    @Arg("interval", {nullable: true, defaultValue: 1000*60*60*24}) interval?: number,
    @Arg("limit", {nullable: true, defaultValue: 1000}) limit?: number,
  ): Promise<PaginatedOHLCResponse> {
    let reverse: boolean = false;
    let args: any[] = [address.address, interval];
    let query: string = "SELECT timestamp, open, high, low, close FROM "+address.coin.keyspace+".ohlc WHERE address=? AND interval=?";
    if (cursor) {
      query += " AND timestamp " + (reverse ? "<" : ">") + " ?";
      args = args.concat([cursor.timestamp]);
    }
    if (reverse) query += " ORDER BY timestamp DESC";
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
    const originalLimit: number = limit;
    let mempool = address.coin.mempool;
    let res: AddressTransaction[];
    if (mempool !== undefined) {
      res = mempool.addressTransactions.get(address.address);
    }
    if (res !== undefined) {
      if (cursor !== undefined) {
        let lastIndex = res.findIndex((e) => {
          if (e.timestamp.getTime() === cursor.timestamp.getTime()) {
            if (e.height === cursor.height) {
              return e.tx_n >= cursor.tx_n;
            }
            return e.height > cursor.height;
          }
          return e.timestamp.getTime() > cursor.timestamp.getTime();
        });//TODO: use binary search instead
        if (lastIndex !== -1) {
          res = res.slice(0, lastIndex);
          if (res.length > limit+1) res = res.slice(res.length-(limit+1));
        }
      }
      res.reverse();
      if (res.length > 0) {
        cursor = { 
          timestamp: res[res.length-1].timestamp,
          height: res[res.length-1].height,
          tx_n: res[res.length-1].tx_n
        }
        limit = limit - res.length;
      }
    } else  {
      res = [];
    }
    if (limit+1 > 0) {

      let args: any[] = [address.address];
      let query: string = "SELECT timestamp, height, tx_n, balance_change FROM "+address.coin.keyspace+".address_transaction WHERE address=?";
      if (cursor) {
        query += " AND (timestamp, height, tx_n) < (?, ?, ?)";
        args = args.concat([cursor.timestamp, cursor.height, cursor.tx_n]);
      }
      let resultSet: types.ResultSet = await this.client.execute(
        query, 
        args, 
        {prepare: true, fetchSize: limit+1}
      );
      let res2: AddressTransaction[] = resultSet.rows.map(row => {
        let addressTransaction = new AddressTransaction();
        addressTransaction.timestamp = row.get("timestamp");
        addressTransaction.height = row.get("height");
        addressTransaction.tx_n = row.get("tx_n");
        addressTransaction.balance_change = row.get("balance_change")/1e8;
        addressTransaction.coin = address.coin;
        return addressTransaction;
      });
      
      if (res2.length > 0) {
        let start = res2[res2.length-1].timestamp;
        let end = res2[0].timestamp;
        let query2: string = "SELECT timestamp, balance FROM "+address.coin.keyspace+".address_balance WHERE address=? AND timestamp >= ? AND timestamp <= ?";
        let args2: any[] = [address.address, start, end];
        let resultSet2: types.ResultSet = await this.client.execute(
          query2, 
          args2, 
          {prepare: true}
        );
        let time2Balance: Map<number, number> = new Map();
        resultSet2.rows.forEach(row => time2Balance.set(row.get("timestamp").getTime(), row.get("balance")/1e8));
        res2.forEach(r => r.balance_after_block = time2Balance.get(r.timestamp.getTime()));
        res = res.concat(res2);
      }

    }
    if (res.length > originalLimit) {
      return {
        hasMore: true,
        items: res.slice(0, originalLimit),
      }
    } else {
      return {
        hasMore: false,
        items: res,
      }
    }
  }

  @FieldResolver({complexity: ({ childComplexity, args }) => 100 + args.limit * childComplexity})
  async addressBalances(@Root() address: Address, 
    @Arg("cursor", {nullable: true}) cursor: AddressBalanceCursor,
    @Arg("limit", {nullable: true, defaultValue: 1000}) limit?: number
  ): Promise<PaginatedAddressBalanceResponse> {
    const originalLimit: number = limit;
    let mempool = address.coin.mempool;
    let res: AddressBalance[];
    if (mempool !== undefined) {
      res = mempool.addressBalances.get(address.address);
    }
    if (res !== undefined) {
      if (cursor !== undefined) {
        let lastIndex = res.findIndex((e) => e.timestamp >= cursor.timestamp);//TODO: use binary search instead
        if (lastIndex !== -1) {
          res = res.slice(0, lastIndex);
        }
      }
      if (res.length > limit+1) res = res.slice(res.length-(limit+1));
      res.reverse();
      if (res.length > 0) {
        cursor = { timestamp: res[res.length-1].timestamp }
        limit = limit - res.length;
      }
    } else  {
      res = [];
    }
    if (limit+1 > 0) {
      let args: any[] = [address.address];
      let query: string = "SELECT timestamp, balance FROM "+address.coin.keyspace+".address_balance WHERE address=?";
      if (cursor) {
        query += " AND timestamp < ?";
        args = args.concat([cursor.timestamp]);
      }
      let resultSet: types.ResultSet = await this.client.execute(
        query, 
        args, 
        {prepare: true, fetchSize: limit+1}
      );
      res = res.concat(resultSet.rows.map(row => {
        let addressBalance = new AddressBalance();
        addressBalance.timestamp = row.get("timestamp");
        addressBalance.balance = row.get("balance")/1e8;
        return addressBalance;
      }));
    }
    if (res.length > originalLimit) {
      return {
        hasMore: true,
        items: res.slice(0, originalLimit),
      }
    } else {
      return {
        hasMore: false,
        items: res,
      }
    }
  };


}