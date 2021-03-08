import { Resolver, FieldResolver, Root } from "type-graphql";
import { types } from "cassandra-driver";
import { Inject } from 'typedi';
import { ConfirmedTransaction } from "../models/confirmed-transaction";
import { ClusterTransaction } from "../models/cluster-transaction";
import { LimitedCapacityClient } from "../limited-capacity-client";

@Resolver(of => ClusterTransaction)
export class ClusterTransactionResolver {


    constructor(@Inject("cassandra_client") private client: LimitedCapacityClient) {
    }

    @FieldResolver(returns => ConfirmedTransaction, { nullable: false, complexity: ({ childComplexity, args }) => 100 + childComplexity })
    async confirmedTransaction(@Root() clusterTransaction: ClusterTransaction,
    ): Promise<ConfirmedTransaction> {
        const args: any[] = [clusterTransaction.height, clusterTransaction.txN];
        const query: string = "SELECT * FROM " + clusterTransaction.coin.keyspace + ".confirmed_transaction WHERE height=? AND tx_n=?";
        const resultSet: types.ResultSet = await this.client.execute(
            query,
            args,
            { prepare: true }
        );
        if (resultSet.rows.length == 0) {
            return null;
        }
        const res: ConfirmedTransaction[] = resultSet.rows.map(row => {
            return <ConfirmedTransaction>{
                height: row.get('height'),
                txN: row.get('tx_n'),
                txid: row.get("txid"),
                coin: clusterTransaction.coin
            };
        });
        return res[0];
    }

}