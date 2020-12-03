import { Resolver, FieldResolver, Root, Field, ArgsType, Args, Arg, Query } from "type-graphql";
import { types, Client } from "cassandra-driver";
import { Inject } from "typedi";
import { Transaction } from "../models/transaction";
import { TransactionInput, TransactionInputCursor, PaginatedTransactionInputResponse } from "../models/transaction-input";
import { TransactionOutput, TransactionOutputCursor, PaginatedTransactionOutputResponse } from "../models/transaction-output";
import { Address } from "../models/address";
import { PaginationArgs } from "./pagination-args";
import { BlockHash } from "../models/block_hash";

@ArgsType()
class TransactionInputArgs extends PaginationArgs {

  @Field({nullable: true})
  cursor: TransactionInputCursor;

}

@ArgsType()
class TransactionOutputArgs extends PaginationArgs {

  @Field({nullable: true})
  cursor: TransactionOutputCursor;

}

@Resolver(of => Transaction)
export class TransactionResolver {

  constructor(@Inject("cassandra_client") private client: Client) {
  }

  @Query(returns => Transaction, {complexity: ({ childComplexity, args }) => 100 + childComplexity})
  async transaction(@Arg("txid") txid: string): Promise<Transaction> {
    let args: any[] = [txid];
    let query: string = "SELECT * FROM transaction WHERE txid=?";
    let resultSet: types.ResultSet = await this.client.execute(
      query, 
      args, 
      {prepare: true}
    );
    let res: Transaction[] = resultSet.rows.map(row => {
      let tx: Transaction = new Transaction();
      tx.txid = row.get('txid');
      tx.locktime = row.get('locktime');
      tx.size = row.get('size');
      tx.version = row.get('version');
      tx.height = row.get('height');
      tx.txN = row.get("tx_n");
      tx.fee = row.get("tx_fee")/1e8;
      return tx;
    });
    return res[0];
  }


  @FieldResolver({complexity: ({ childComplexity, args }) => 100 + childComplexity})
  async blockHash(@Root() transaction: Transaction, 
  ): Promise<BlockHash> {
    if (transaction.height === undefined || transaction.height === null) return null;
    let args: any[] = [transaction.height];
    let query: string = "SELECT * FROM longest_chain WHERE height=?";
    let resultSet: types.ResultSet = await this.client.execute(
      query, 
      args, 
      {prepare: true}
    );
    let res: BlockHash[] = resultSet.rows.map(row => {
      let b: BlockHash = new BlockHash();
      b.hash = row.get('hash');
      b.height = row.get('height');
      return b;
    });
    return res[0];
  }



  @FieldResolver( {complexity: ({ childComplexity, args }) => 100 + args.limit * childComplexity})
  async vin(@Root() transaction: Transaction, 
    @Args() {cursor, limit}: TransactionInputArgs
  ): Promise<PaginatedTransactionInputResponse> {
    let args: any[] = [transaction.txid];
    let query: string = "SELECT * FROM transaction_input WHERE spending_txid=?";
    if (cursor) {
      query += " AND spending_index > ?";
      args = args.concat([cursor.spending_index]);
    }
    let resultSet: types.ResultSet = await this.client.execute(
      query, 
      args, 
      {prepare: true, fetchSize: limit}
    );
    let res: TransactionInput[] = resultSet.rows.map(row => {
      let vin: TransactionInput = new TransactionInput();
      vin.coinbase = row.get('coinbase');
      vin.scriptsig = row.get('scriptsig');
      vin.sequence = row.get('sequence');
      vin.txid = row.get('txid');
      vin.vout = row.get('vout');
      vin.spending_txid = row.get('spending_txid');
      vin.spending_index = row.get('spending_index');
      return vin;
    });
    return {
      hasMore: resultSet.pageState !== null,
      items: res
    };
  }

  @FieldResolver( {complexity: ({ childComplexity, args }) => 100 + args.limit * childComplexity})
  async vout(@Root() transaction: Transaction, 
    @Args() {cursor, limit}: TransactionOutputArgs
  ): Promise<PaginatedTransactionOutputResponse> {
    let args: any[] = [transaction.txid];
    let query: string = "SELECT * FROM transaction_output WHERE txid=?";
    if (cursor) {
      query += " AND n > ?";
      args = args.concat([cursor.n]);
    }
    let resultSet: types.ResultSet = await this.client.execute(
      query, 
      args, 
      {prepare: true, fetchSize: limit}
    );
    let res: TransactionOutput[] = resultSet.rows.map(row => {
      let vout: TransactionOutput = new TransactionOutput();
      vout.txid = row.get('txid');
      vout.n = row.get('n');
      vout.value = row.get('value');
      let scriptpubkey = row.get('scriptpubkey');
      if (scriptpubkey.addresses !== undefined && scriptpubkey.addresses !== null) {
        scriptpubkey.addresses = scriptpubkey.addresses.map(address => new Address(address));
      }
      vout.scriptpubkey = scriptpubkey;
      vout.spending_txid = row.get('spending_txid');
      vout.spending_index = row.get('spending_index');
      return vout;
    });
    return {
      hasMore: resultSet.pageState !== null,
      items: res
    };
  }

}