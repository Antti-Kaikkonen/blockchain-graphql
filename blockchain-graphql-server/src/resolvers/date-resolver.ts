import { Resolver, Args, ArgsType, Field, FieldResolver, Root } from "type-graphql";
import { Date } from "../models/date";
import { types } from "cassandra-driver";
import { RichListCursor, Richlist, PaginatedRichlistResponse } from "../models/richlist";
import { Address } from "../models/address";
import { Inject } from "typedi";
import { AddressBalanceChange, AddressBalanceChangeCursor, PaginatedAddressBalanceChangeResponse } from "../models/address-balance-change";
import { PaginationArgs } from "./pagination-args";
import { LimitedCapacityClient } from "../limited-capacity-client";
import { AddressCluster } from "../models/address-cluster";
import { PaginatedAddressClusterBalanceChangeResponse, AddressClusterBalanceChangeCursor, AddressClusterBalanceChange } from "../models/address-cluster-balance-change";

@ArgsType()
class RichlistArgs extends PaginationArgs {

  @Field({nullable: true})
  cursor: RichListCursor;

}

@ArgsType()
class AddressBalanceChangeArgs extends PaginationArgs {

  @Field({nullable: true})
  cursor: AddressBalanceChangeCursor;

}

@ArgsType()
class AddressClusterBalanceChangeArgs extends PaginationArgs {

  @Field({nullable: true})
  cursor: AddressClusterBalanceChangeCursor;

}

@Resolver(of => Date)
export class DateResolver {

  static BIN_COUNT: number = 20;
  static BINS: number[] = Array.from(new Array(DateResolver.BIN_COUNT).keys());

  constructor(@Inject("cassandra_client") private client: LimitedCapacityClient) {
  }

  @FieldResolver({complexity: ({ childComplexity, args }) => 100 + args.limit * childComplexity})
  async richlist(
    @Root() date: Date, 
    @Args() {cursor, limit}: RichlistArgs
  ): Promise<PaginatedRichlistResponse> {
    let args: any[] = [date.date, DateResolver.BINS];
    let query: string = "SELECT balance, balance_change, address FROM "+date.coin.keyspace+".daily_richlist WHERE date=? AND bin IN ?";
    if (cursor) {
      query += " AND (balance, balance_change, address) < (?, ?, ?)"
      args = args.concat([Math.round(cursor.balance), Math.round(cursor.balance_change), cursor.address]);
    }
    args.push(limit+1);
    query += " ORDER BY balance DESC, balance_change DESC, address DESC LIMIT ?";
    let resultSet: types.ResultSet = await this.client.execute(
      query, 
      args, 
      {prepare: true, fetchSize: null}
    );
    let hasMore: boolean = resultSet.rows.length > limit;
    if (hasMore) resultSet.rows.pop();
    let res: Richlist[] = resultSet.rows.map(row => {
        let richlist: Richlist = new Richlist();
        let address = new Address(row.get("address"), date.coin);
        address.coin = date.coin;
        richlist.address = address;
        richlist.balance = row.get("balance");
        richlist.balance_change = row.get("balance_change");
        return richlist;
    });
    return {
        items: res, 
        hasMore: hasMore
    };
  }

  @FieldResolver( {complexity: ({ childComplexity, args }) => 100 + args.limit * childComplexity})
  async top_gainers(@Root() date: Date, 
    @Args() {cursor, limit}: AddressBalanceChangeArgs
  ): Promise<PaginatedAddressBalanceChangeResponse> {
    let reverse: boolean = false;
    let args: any[] = [date.date, DateResolver.BINS];
    let query: string = "SELECT address, balance_change FROM "+date.coin.keyspace+".daily_top_gainers WHERE date=? AND bin IN ?";
    if (cursor) {
      query += " AND (balance_change, address) " + (reverse ? ">" : "<") + " (?, ?)";
      args = args.concat([cursor.balance_change, cursor.address]);
    }
    args.push(limit+1);
    if (reverse) {
      query += " ORDER BY balance_change ASC, address ASC LIMIT ?";
    } else {
      query += " ORDER BY balance_change DESC, address DESC LIMIT ?";
    }
    let resultSet: types.ResultSet = await this.client.execute(
      query, 
      args, 
      {prepare: true, fetchSize: null}
    );
    let hasMore: boolean = resultSet.rows.length > limit;
    if (hasMore) resultSet.rows.pop();
    let res: AddressBalanceChange[] = resultSet.rows.map(row => {
        let adressBalanceChange = new AddressBalanceChange();
        let address = new Address(row.get("address"), date.coin);
        adressBalanceChange.address = address;
        adressBalanceChange.balance_change = row.get("balance_change");
        return adressBalanceChange;
    });
    return {
        items: res,
        hasMore: hasMore
    };
  }

  @FieldResolver({complexity: ({ childComplexity, args }) => args.limit * childComplexity})
  async top_losers(@Root() date: Date, 
    @Args() {cursor, limit}: AddressBalanceChangeArgs
  ): Promise<PaginatedAddressBalanceChangeResponse> {
    let reverse: boolean = false;
    let args: any[] = [date.date, DateResolver.BINS];
    let query: string = "SELECT address, balance_change FROM "+date.coin.keyspace+".daily_top_losers WHERE date=? AND bin IN ?";
    if (cursor) {
      query += " AND (balance_change, address) " + (reverse ? "<" : ">") + " (?, ?)";
      args = args.concat([Math.round(cursor.balance_change), cursor.address]);
    }
    args.push(limit+1);
    if (reverse) {
      query += " ORDER BY balance_change DESC, address DESC LIMIT ?";
    } else {
      query += " ORDER BY balance_change ASC, address ASC LIMIT ?";
    }
    let resultSet: types.ResultSet = await this.client.execute(
      query, 
      args, 
      {prepare: true, fetchSize: null}
    );
    let hasMore: boolean = resultSet.rows.length > limit;
    if (hasMore) resultSet.rows.pop();
    let res: AddressBalanceChange[] = resultSet.rows.map(row => {
        let adressBalanceChange = new AddressBalanceChange();
        let address = new Address(row.get("address"), date.coin);
        adressBalanceChange.address = address;
        adressBalanceChange.balance_change = row.get("balance_change");
        return adressBalanceChange;
    });
    return {
        items: res,
        hasMore: hasMore
    };
  }

  @FieldResolver(returns => PaginatedAddressClusterBalanceChangeResponse, {complexity: ({ childComplexity, args }) => 100 + args.limit * childComplexity})
  async top_cluster_gainers(@Root() date: Date, 
    @Args() {cursor, limit}: AddressClusterBalanceChangeArgs
  ): Promise<PaginatedAddressClusterBalanceChangeResponse> {
    let args: any[] = [date.date, DateResolver.BINS];
    let query: string = "SELECT balance_change, cluster_id FROM "+date.coin.keyspace+".daily_top_cluster_gainers WHERE date=? AND bin IN ?";
    if (cursor) {
      query += " AND (balance_change, cluster_id) < " + " (?, ?)";
      args = args.concat([cursor.balance_change, cursor.clusterId]);
    }
    args.push(limit+1); 
    query += " ORDER BY balance_change DESC, cluster_id DESC LIMIT ?";
    let resultSet: types.ResultSet = await this.client.execute(
      query, 
      args, 
      {prepare: true, fetchSize: null}
    );
    let hasMore: boolean = resultSet.rows.length > limit;
    if (hasMore) resultSet.rows.pop();
    let res: AddressClusterBalanceChange[] = resultSet.rows.map(row => {
        let clusterBalanceChange = new AddressClusterBalanceChange();
        clusterBalanceChange.guestimatedWallet = new AddressCluster();
        clusterBalanceChange.guestimatedWallet.clusterId = row.get("cluster_id");
        clusterBalanceChange.guestimatedWallet.coin = date.coin;
        clusterBalanceChange.balance_change = row.get("balance_change");
        return clusterBalanceChange;
    });
    return {
        items: res,
        hasMore: hasMore
    };
  }

  @FieldResolver(returns => PaginatedAddressClusterBalanceChangeResponse, {complexity: ({ childComplexity, args }) => 100 + args.limit * childComplexity})
  async top_cluster_losers(@Root() date: Date, 
    @Args() {cursor, limit}: AddressClusterBalanceChangeArgs
  ): Promise<PaginatedAddressClusterBalanceChangeResponse> {
    let args: any[] = [date.date, DateResolver.BINS];
    let query: string = "SELECT balance_change, cluster_id FROM "+date.coin.keyspace+".daily_top_cluster_losers WHERE date=? AND bin IN ?";
    if (cursor) {
      query += " AND (balance_change, cluster_id) > " + " (?, ?)";
      args = args.concat([cursor.balance_change, cursor.clusterId]);
    }
    args.push(limit+1); 
    query += " ORDER BY balance_change ASC, cluster_id ASC LIMIT ?";
    let resultSet: types.ResultSet = await this.client.execute(
      query, 
      args, 
      {prepare: true, fetchSize: null}
    );
    let hasMore: boolean = resultSet.rows.length > limit;
    if (hasMore) resultSet.rows.pop();
    let res: AddressClusterBalanceChange[] = resultSet.rows.map(row => {
        let clusterBalanceChange = new AddressClusterBalanceChange();
        clusterBalanceChange.guestimatedWallet = new AddressCluster();
        clusterBalanceChange.guestimatedWallet.clusterId = row.get("cluster_id");
        clusterBalanceChange.guestimatedWallet.coin = date.coin;
        clusterBalanceChange.balance_change = row.get("balance_change");
        return clusterBalanceChange;
    });
    return {
        items: res,
        hasMore: hasMore
    };
  }

}