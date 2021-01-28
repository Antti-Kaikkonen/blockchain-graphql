import { AddressCluster } from "../models/address-cluster";
import { Resolver, FieldResolver, Root, InputType, Field, Args, ArgsType } from "type-graphql";
import { Inject } from "typedi";
import { types } from "cassandra-driver";
import { PaginatedClusterTransactionResponse, ClusterTransactionCursor, ClusterTransaction } from "../models/cluster-transaction";
import { PaginatedAddressResponse, Address } from "../models/address";
import { LimitedCapacityClient } from "../limited-capacity-client";
import { AddressClusterDailyBalanceChange, AddressClusterDailyBalanceChangeCursor, PaginatedAddressClusterDailyBalanceChangeResponse } from "../models/address-cluster-daily-balance-change";
import { AddressClusterDetails } from "../models/address-cluster-details";
import { CoinResolver } from './coin-resolver';
import { PaginationArgs } from "./pagination-args";

@InputType()
export class AddressCursor {

  @Field({nullable: false})
  address: string;

}

@ArgsType()
class ClusterTransactionsArgs extends PaginationArgs {

  @Field({nullable: true})
  cursor: ClusterTransactionCursor;

}

@ArgsType()
class DailyBalanceChangeArgs extends PaginationArgs {

  @Field({nullable: true})
  cursor: AddressClusterDailyBalanceChangeCursor;
  
}

@ArgsType()
class ClusterAddressesArgs extends PaginationArgs {
  @Field({nullable: true})
  cursor: AddressCursor;
}

function hashCode(s) {
  let h;
  for(let i = 0; i < s.length; i++) 
        h = Math.imul(31, h) + s.charCodeAt(i) | 0;

  return h;
}

@Resolver(of => AddressCluster)
export class AddressClusterResolver {

  constructor(@Inject("cassandra_client") private client: LimitedCapacityClient) {
  }

  static CLUSTER_DAILY_BALANCES_BIN_COUNT: number = 20;

  @FieldResolver({complexity: ({ childComplexity, args }) => 100 + args.limit * childComplexity})
  async clusterTransactions(@Root() cluster: AddressCluster, 
    @Args() {limit, cursor}: ClusterTransactionsArgs
  ): Promise<PaginatedClusterTransactionResponse> {
    let args: any[] = [cluster.clusterId];
    let query: string = "SELECT timestamp, height, tx_n, balance_change FROM "+cluster.coin.keyspace+".cluster_transaction WHERE cluster_id=?";
    if (cursor) {
      query += " AND (timestamp, height, tx_n) < (?, ?, ?)";
      args = args.concat([cursor.timestamp, cursor.height, cursor.tx_n]);
    }
    let resultSet: types.ResultSet = await this.client.execute(
      query, 
      args, 
      {prepare: true, fetchSize: limit}
    );
    let res: ClusterTransaction[] = resultSet.rows.map(row => {
      let clusterTransaction = new ClusterTransaction();
      clusterTransaction.timestamp = row.get("timestamp");
      clusterTransaction.height = row.get("height");
      clusterTransaction.tx_n = row.get("tx_n");
      clusterTransaction.balance_change = row.get("balance_change");
      clusterTransaction.coin = cluster.coin;
      return clusterTransaction;
    });
    return {
      hasMore: resultSet.pageState !== null,
      items: res,
    };
  }

  @FieldResolver({complexity: ({ childComplexity, args }) => 100 + args.limit * childComplexity})
  async clusterAddresses(@Root() cluster: AddressCluster, 
    @Args() {limit, cursor}: ClusterAddressesArgs
  ): Promise<PaginatedAddressResponse> {
    let args: any[] = [cluster.clusterId];
    let query: string = "SELECT address FROM "+cluster.coin.keyspace+".cluster_address WHERE cluster_id=?";
    if (cursor) {
      query += " AND address > ?";
      args = args.concat([cursor.address]);
    }
    let resultSet: types.ResultSet = await this.client.execute(
      query, 
      args, 
      {prepare: true, fetchSize: limit}
    );
    let res: Address[] = resultSet.rows.map(row => new Address(row.get("address"), cluster.coin));
    return {
      hasMore: resultSet.pageState !== null,
      items: res,
    };
  }

  @FieldResolver(returns => PaginatedAddressClusterDailyBalanceChangeResponse, {complexity: ({ childComplexity, args }) => 100 + args.limit * childComplexity})
  async dailyBalanceChanges(@Root() cluster: AddressCluster, 
    @Args() {limit, cursor}: DailyBalanceChangeArgs
  ): Promise<PaginatedAddressClusterDailyBalanceChangeResponse> {
    let bin = Math.abs(hashCode(cluster.clusterId)) % AddressClusterResolver.CLUSTER_DAILY_BALANCES_BIN_COUNT;
    let args: any[] = [cluster.clusterId, bin];
    let query: string = "SELECT date, balance_change FROM "+cluster.coin.keyspace+".cluster_daily_balance_change WHERE cluster_id=? AND bin = ?";
    if (cursor) {
      query += " AND date > ?";
      args = args.concat([cursor.date]);
    }
    args.push(limit+1); 
    query += " ORDER BY date ASC LIMIT ?";
    let resultSet: types.ResultSet = await this.client.execute(
      query, 
      args, 
      {prepare: true, fetchSize: null}
    );
    let hasMore: boolean = resultSet.rows.length > limit;
    if (hasMore) resultSet.rows.pop();
    let res: AddressClusterDailyBalanceChange[] = resultSet.rows.map(row => {
      let e = new AddressClusterDailyBalanceChange();
      e.date = row.get("date");
      e.balance_change = row.get("balance_change");
      return e;
    });
    return {
      hasMore: hasMore,
      items: res,
    };
  }

  @FieldResolver(returns => AddressClusterDetails, {nullable: true, complexity: ({ childComplexity, args }) => 100})
  async details(@Root() cluster: AddressCluster, 
  ): Promise<AddressClusterDetails> {
    let bin = Math.abs(hashCode(cluster.clusterId)) % CoinResolver.CLUSTER_RICHLIST_BIN_COUNT;
    let args: any[] = [cluster.clusterId, bin];
    let query: string = "SELECT * FROM "+cluster.coin.keyspace+".cluster_details WHERE cluster_id=? AND bin = ?";
    let resultSet: types.ResultSet = await this.client.execute(
      query, 
      args, 
      {prepare: true, fetchSize: null}
    );
    let res: AddressClusterDetails[] = resultSet.rows.map(row => {
      let e = new AddressClusterDetails();
      e.balance = row.get("balance");
      return e;
    });
    return res[0];
  }

}