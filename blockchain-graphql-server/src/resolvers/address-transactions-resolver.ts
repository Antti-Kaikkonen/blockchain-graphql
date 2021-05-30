import { Resolver, FieldResolver, Root } from "type-graphql"
import { types } from "cassandra-driver"
import { Inject } from 'typedi'
import { AddressTransaction } from "../models/address-transaction"
import { ConfirmedTransaction } from "../models/confirmed-transaction"
import { LimitedCapacityClient } from "../limited-capacity-client"
import { MempoolBlock } from "../mempool/mempool"

@Resolver(of => AddressTransaction)
export class AddressTransactionsResolver {


    constructor(@Inject("cassandra_client") private client: LimitedCapacityClient) {
    }

    @FieldResolver(returns => ConfirmedTransaction, { nullable: false, complexity: ({ childComplexity, args }) => 100 + childComplexity })
    async confirmedTransaction(@Root() addressTransaction: AddressTransaction,
    ): Promise<ConfirmedTransaction> {
        const mempoolBlock: MempoolBlock = addressTransaction.coin.mempool?.blockByHeight.get(addressTransaction.height)
        if (mempoolBlock !== undefined) {
            const tx = mempoolBlock.tx[addressTransaction.txN]
            return <ConfirmedTransaction>{
                height: tx.height,
                txN: tx.txN,
                txid: tx.rpcTx.txid,
                coin: addressTransaction.coin
            }
        }
        const args: any[] = [addressTransaction.height, addressTransaction.txN]
        const query: string = "SELECT * FROM " + addressTransaction.coin.keyspace + ".confirmed_transaction WHERE height=? AND tx_n=?"
        const resultSet: types.ResultSet = await this.client.execute(
            query,
            args,
            { prepare: true }
        )
        if (resultSet.rows.length == 0) {
            return null
        }
        const res: ConfirmedTransaction[] = resultSet.rows.map(row => {
            return <ConfirmedTransaction>{
                height: row.get("height"),
                txN: row.get("tx_n"),
                txid: row.get("txid"),
                coin: addressTransaction.coin
            }
        })
        return res[0]
    }

}