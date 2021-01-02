import { Resolver, FieldResolver, Root } from "type-graphql";
import { types } from "cassandra-driver";
import { Inject } from 'typedi';
import { ConfirmedTransaction } from "../models/confirmed-transaction";
import { ClusterTransaction } from "../models/cluster-transaction";
import { LimitedCapacityClient } from "../limited-capacity-client";

@Resolver(of => ClusterTransaction)
export class ClusterTransactionResolver {


  constructor(@Inject("cassandra_client") private client: LimitedCapacityClient ) {
  }

  @FieldResolver({complexity: ({ childComplexity, args }) => 100 + childComplexity})
  async confirmedTransaction(@Root() clusterTransaction: ClusterTransaction, 
  ): Promise<ConfirmedTransaction> {
    let args: any[] = [clusterTransaction.height, clusterTransaction.tx_n];
    let query: string = "SELECT * FROM "+clusterTransaction.coin.keyspace+".confirmed_transaction WHERE height=? AND tx_n=?";
    let resultSet: types.ResultSet = await this.client.execute(
      query, 
      args, 
      {prepare: true}
    );
    if (resultSet.rows.length == 0) {
      return null;
    }
    let res: ConfirmedTransaction[] = resultSet.rows.map(row => {
      let tx: ConfirmedTransaction = new ConfirmedTransaction();
      tx.height = row.get('height');
      tx.tx_n = row.get('tx_n');
      tx.txid = row.get("txid");
      tx.coin = clusterTransaction.coin;
      return tx;
    });
    return res[0];
  }

}