import { Resolver, Query, Arg, Args, Int, ArgsType, Field, FieldResolver, Root } from "type-graphql";
import { Date } from "../models/date";
import { Client, types } from "cassandra-driver";
import { RichListCursor, Richlist, PaginatedRichlistResponse } from "../models/richlist";
import { Address } from "../models/address";
import { Inject } from "typedi";
import { AddressBalanceChange, AddressBalanceChangeCursor, PaginatedAddressBalanceChangeResponse } from "../models/address-balance-change";
import { PaginationArgs } from "./pagination-args";
import { stdout } from "process";

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

@Resolver(of => Date)
export class DateResolver {

  static BIN_COUNT: number = 10;
  static BINS: number[] = Array.from(new Array(DateResolver.BIN_COUNT).keys());

  constructor(@Inject("cassandra_client") private client: Client) {
  }

  @Query(returns => Date, {complexity: 1})
  async date(@Arg("date") date: string) {
    let res = new Date();
    res.date = date;
    return res;
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
      args = args.concat([Math.round(cursor.balance*1e8), Math.round(cursor.balance_change*1e8), cursor.address]);
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
        richlist.balance = row.get("balance")/1e8;
        richlist.balance_change = row.get("balance_change")/1e8;
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
      args = args.concat([Math.round(cursor.balance_change*1e8), cursor.address]);
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
        adressBalanceChange.balance_change = row.get("balance_change")/1e8;
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
      args = args.concat([Math.round(cursor.balance_change*1e8), cursor.address]);
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
        adressBalanceChange.balance_change = row.get("balance_change")/1e8;
        return adressBalanceChange;
    });
    return {
        items: res,
        hasMore: hasMore
    };
  }

}