import { Resolver, FieldResolver, Root } from "type-graphql";
import { types } from "cassandra-driver";
import { Inject } from 'typedi';
import { AddressTransaction } from "../models/address-transaction";
import { ConfirmedTransaction } from "../models/confirmed-transaction";
import { LimitedCapacityClient } from "../limited-capacity-client";

@Resolver(of => AddressTransaction)
export class AddressTransactionsResolver {


  constructor(@Inject("cassandra_client") private client: LimitedCapacityClient) {
  }

  @FieldResolver(returns => ConfirmedTransaction, {nullable: false, complexity: ({ childComplexity, args }) => 100 + childComplexity})
  async confirmedTransaction(@Root() addressTransaction: AddressTransaction, 
  ): Promise<ConfirmedTransaction> {
    //TODO: check mempool first
    let args: any[] = [addressTransaction.height, addressTransaction.txN];
    let query: string = "SELECT * FROM "+addressTransaction.coin.keyspace+".confirmed_transaction WHERE height=? AND tx_n=?";
    let resultSet: types.ResultSet = await this.client.execute(
      query, 
      args, 
      {prepare: true}
    );
    if (resultSet.rows.length == 0) {
      return null;
    }
    let res: ConfirmedTransaction[] = resultSet.rows.map(row => {
      return <ConfirmedTransaction>{
        height: row.get("height"), 
        txN: row.get("tx_n"), 
        txid: row.get("txid"), 
        coin: addressTransaction.coin
      };
    });
    return res[0];
  }

}