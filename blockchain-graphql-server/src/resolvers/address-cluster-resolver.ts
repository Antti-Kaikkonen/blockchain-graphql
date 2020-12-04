import { AddressCluster } from "../models/address-cluster";
import { Resolver, FieldResolver, Root, Arg, InputType, Field } from "type-graphql";
import { Inject } from "typedi";
import { Client, types } from "cassandra-driver";
import { PaginatedClusterTransactionResponse, ClusterTransactionCursor, ClusterTransaction } from "../models/cluster-transaction";
import { PaginatedAddressResponse, Address } from "../models/address";
import { Coin } from "../models/coin";

@InputType()
export class AddressCursor {

  @Field({nullable: false})
  address: string;

}

@Resolver(of => AddressCluster)
export class AddressClusterResolver {

  constructor(@Inject("cassandra_client") private client: Client) {
  }

  @FieldResolver({complexity: ({ childComplexity, args }) => 100 + args.limit * childComplexity})
  async clusterTransactions(@Root() cluster: AddressCluster, 
    @Arg("cursor", {nullable: true}) cursor: ClusterTransactionCursor,
    @Arg("limit", {nullable: true, defaultValue: 100}) limit?: number, 
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
      clusterTransaction.balance_change = row.get("balance_change")/1e8;
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
    @Arg("cursor", {nullable: true}) cursor: AddressCursor,
    @Arg("limit", {nullable: true, defaultValue: 100}) limit?: number, 
  ): Promise<PaginatedAddressResponse> {
    let args: any[] = [cluster.clusterId];
    let query: string = "SELECT address FROM "+cluster.coin.keyspace+".cluster_address WHERE cluster_id=?";
    if (cursor) {
      query += " AND (address) < (?)";
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

}