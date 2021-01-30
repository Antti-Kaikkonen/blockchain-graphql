import { Resolver, FieldResolver, Root, Field, ArgsType, Args, Arg, Query } from "type-graphql";
import { types } from "cassandra-driver";
import { Inject } from "typedi";
import { Transaction } from "../models/transaction";
import { TransactionInput, TransactionInputCursor, PaginatedTransactionInputResponse } from "../models/transaction-input";
import { TransactionOutput, TransactionOutputCursor, PaginatedTransactionOutputResponse } from "../models/transaction-output";
import { Address } from "../models/address";
import { PaginationArgs } from "./pagination-args";
import { BlockHash } from "../models/block_hash";
import { MempoolTx } from "../mempool";
import { RpcVout } from "../rpc-client";
import { ScriptPubKey } from "../models/scriptpubkey";
import { LimitedCapacityClient } from "../limited-capacity-client";

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

  constructor(
    @Inject("cassandra_client") private client: LimitedCapacityClient
  ) {}

  @FieldResolver({complexity: ({ childComplexity, args }) => 100 + childComplexity})
  async blockHash(@Root() transaction: Transaction, 
  ): Promise<BlockHash> {
    if (transaction.height === undefined || transaction.height === null) return null;
    let mempool = transaction.coin.mempool;
    let mempoolBlock = mempool === undefined ? undefined : mempool.blockByHeight.get(transaction.height);
    if (mempoolBlock !== undefined) {
      let res: BlockHash = new BlockHash();
      res.hash = mempoolBlock.hash;
      res.height = mempoolBlock.height;
      res.coin = transaction.coin;
      return res;
    }
    let args: any[] = [transaction.height];
    let query: string = "SELECT * FROM "+transaction.coin.keyspace+".longest_chain WHERE height=?";
    let resultSet: types.ResultSet = await this.client.execute(
      query, 
      args, 
      {prepare: true}
    );
    let res: BlockHash[] = resultSet.rows.map(row => {
      let b: BlockHash = new BlockHash();
      b.hash = row.get('hash');
      b.height = row.get('height');
      b.coin = transaction.coin;
      return b;
    });
    return res[0];
  }



  @FieldResolver(returns => PaginatedTransactionInputResponse, {complexity: ({ childComplexity, args }) => 100 + args.limit * childComplexity})
  async inputs(@Root() transaction: Transaction, 
    @Args() {cursor, limit}: TransactionInputArgs
  ): Promise<PaginatedTransactionInputResponse> {
    let mempool = transaction.coin.mempool;
    let mempoolTx = mempool === undefined ? undefined : mempool.txById.get(transaction.txid);
    if (mempoolTx !== undefined) {
      let res: TransactionInput[] = [];
      let fromIndex = cursor === undefined ? 0 : cursor.spendingIndex+1;
      for (let spending_index = fromIndex; spending_index < mempoolTx.vin.length; spending_index++) {
        if (res.length == limit) {
          return {
            hasMore: true,
            items: res
          };
        }
        let rpcVin = mempoolTx.vin[spending_index];
        let vin: TransactionInput = new TransactionInput();
        vin.coinbase = rpcVin.coinbase;
        vin.scriptSig = rpcVin.scriptSig;
        vin.sequence = rpcVin.sequence;
        vin.txid = rpcVin.txid;
        vin.vout = rpcVin.vout;
        vin.spendingTxid = mempoolTx.txid;
        vin.spendingIndex = spending_index;
        vin.coin = transaction.coin;
        res.push(vin);
      }
      return {
        hasMore: false,
        items: res
      };
    }
    
    let args: any[] = [transaction.txid];
    let query: string = "SELECT * FROM "+transaction.coin.keyspace+".transaction_input WHERE spending_txid=?";
    if (cursor) {
      query += " AND spending_index > ?";
      args = args.concat([cursor.spendingIndex]);
    }
    query += " LIMIT ?"
    args.push(limit+1);
    let resultSet: types.ResultSet = await this.client.execute(
      query, 
      args, 
      {prepare: true, fetchSize: null}
    );
    let hasMore: boolean = resultSet.rows.length > limit;
    if (hasMore) resultSet.rows.pop();
    
    let res: TransactionInput[] = resultSet.rows.map(row => {
      let vin: TransactionInput = new TransactionInput();
      vin.coinbase = row.get('coinbase');
      vin.scriptSig = row.get('scriptsig');
      vin.sequence = row.get('sequence');
      vin.txid = row.get('txid');
      vin.vout = row.get('vout');
      vin.spendingTxid = row.get('spending_txid');
      vin.spendingIndex = row.get('spending_index');
      vin.coin = transaction.coin;
      return vin;
    });
    return {
      hasMore: hasMore,
      items: res
    };
  }

  @FieldResolver(returns => PaginatedTransactionOutputResponse, {complexity: ({ childComplexity, args }) => 100 + args.limit * childComplexity})
  async outputs(@Root() transaction: Transaction, 
    @Args() {cursor, limit}: TransactionOutputArgs
  ): Promise<PaginatedTransactionOutputResponse> {
    let mempool = transaction.coin.mempool;
    let mempoolTx: MempoolTx = mempool === undefined ? undefined : mempool.txById.get(transaction.txid);
    if (mempoolTx !== undefined) {
      let res: TransactionOutput[] = [];
      let fromIndex = cursor === undefined ? 0 : cursor.n+1;
      for (let n = fromIndex; n < mempoolTx.vout.length; n++) {
        if (res.length == limit) {
          return {
            hasMore: true,
            items: res
          };
        }
        let rpcVout: RpcVout = mempoolTx.vout[n];
        let vout: TransactionOutput = new TransactionOutput();
        vout.txid = mempoolTx.txid;
        vout.n = n;
        vout.value = rpcVout.value;

        let scriptpubkey: ScriptPubKey = new ScriptPubKey();// = rpcVout.scriptPubKey;
        scriptpubkey.asm = rpcVout.scriptPubKey.asm;
        scriptpubkey.hex = rpcVout.scriptPubKey.hex;
        scriptpubkey.reqSigs = rpcVout.scriptPubKey.reqSigs;
        scriptpubkey.type = rpcVout.scriptPubKey.type;
        if (rpcVout.scriptPubKey.addresses !== undefined && rpcVout.scriptPubKey.addresses !== null) {
          scriptpubkey.addresses = rpcVout.scriptPubKey.addresses.map(address => new Address(address, transaction.coin));
        }
        vout.scriptPubKey = scriptpubkey;
        
        let spending_inpoint = mempool === undefined ? undefined : mempool.outpointToInpoint.get(vout.txid+vout.n);
        if (spending_inpoint !== undefined) {
          vout.spendingTxid = spending_inpoint.spending_txid;
          vout.spendingIndex = spending_inpoint.spending_index;
        }

        //vout.spending_txid = row.get('spending_txid');
        //vout.spending_index = row.get('spending_index');
        vout.coin = transaction.coin;
        res.push(vout);
      }
      return {
        hasMore: false,
        items: res
      };
    }  

    let args: any[] = [transaction.txid];
    let query: string = "SELECT * FROM "+transaction.coin.keyspace+".transaction_output WHERE txid=?";
    if (cursor) {
      query += " AND n > ?";
      args = args.concat([cursor.n]);
    }
    query += " LIMIT ?"
    args.push(limit+1);
    let resultSet: types.ResultSet = await this.client.execute(
      query, 
      args, 
      {prepare: true, fetchSize: limit}
    );
    let hasMore: boolean = resultSet.rows.length > limit;
    if (hasMore) resultSet.rows.pop();
    let res: TransactionOutput[] = resultSet.rows.map(row => {
      let vout: TransactionOutput = new TransactionOutput();
      vout.txid = row.get('txid');
      vout.n = row.get('n');
      vout.value = row.get('value');
      let scriptpubkey = row.get('scriptpubkey');
      if (scriptpubkey.addresses !== undefined && scriptpubkey.addresses !== null) {
        scriptpubkey.addresses = scriptpubkey.addresses.map(address => new Address(address, transaction.coin));
      }
      vout.scriptPubKey = scriptpubkey;
      vout.spendingTxid = row.get('spending_txid');
      vout.spendingIndex = row.get('spending_index');
      if (vout.spendingTxid === undefined || vout.spendingTxid === null) {
        let spending_inpoint = mempool === undefined ? undefined : mempool.outpointToInpoint.get(vout.txid+vout.n);
        if (spending_inpoint !== undefined) {
          vout.spendingTxid = spending_inpoint.spending_txid;
          vout.spendingIndex = spending_inpoint.spending_index;
        }
      }
      vout.coin = transaction.coin;
      return vout;
    });
    return {
      hasMore: hasMore,
      items: res
    };
  }

}