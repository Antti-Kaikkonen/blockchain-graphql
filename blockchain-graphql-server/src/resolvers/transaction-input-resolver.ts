import { Resolver, FieldResolver, Root } from "type-graphql";
import { types } from "cassandra-driver";
import { Inject } from "typedi";
import { Transaction } from "../models/transaction";
import { TransactionInput } from "../models/transaction-input";
import { TransactionOutput } from "../models/transaction-output";
import { Address } from "../models/address";
import { ScriptPubKey } from "../models/scriptpubkey";
import { RpcVout } from "../rpc-client";
import { LimitedCapacityClient } from "../limited-capacity-client";

@Resolver(of => TransactionInput)
export class TransactionInputResolver {

  constructor(
    @Inject("cassandra_client") private client: LimitedCapacityClient
  ) {}

  @FieldResolver( {complexity: ({ childComplexity, args }) => 100 + childComplexity})
  async spentOutput(@Root() transactionInput: TransactionInput, 
  ): Promise<TransactionOutput> {
    if (transactionInput.txid === null || transactionInput.txid === undefined) return null;
    transactionInput.coin.mempool
    let mempool = transactionInput.coin.mempool;
    let mempoolTx = mempool === undefined ? undefined : mempool.txById.get(transactionInput.txid);
    if (mempoolTx !== undefined) {
      let rpcVout: RpcVout = mempoolTx.vout[transactionInput.vout];
      let vout: TransactionOutput = new TransactionOutput();
      vout.txid = mempoolTx.txid;
      vout.n = rpcVout.n;
      vout.value = rpcVout.value;

      let scriptpubkey: ScriptPubKey = new ScriptPubKey();// = rpcVout.scriptPubKey;
      scriptpubkey.asm = rpcVout.scriptPubKey.asm;
      scriptpubkey.hex = rpcVout.scriptPubKey.hex;
      scriptpubkey.reqsigs = rpcVout.scriptPubKey.reqSigs;
      scriptpubkey.type = rpcVout.scriptPubKey.type;
      if (rpcVout.scriptPubKey.addresses !== undefined && rpcVout.scriptPubKey.addresses !== null) {
        scriptpubkey.addresses = rpcVout.scriptPubKey.addresses.map(address => new Address(address, transactionInput.coin));
      }
      vout.scriptpubkey = scriptpubkey;

      let spending_inpoint = mempool === undefined ? undefined : mempool.outpointToInpoint.get(vout.txid+vout.n);
      if (spending_inpoint !== null) {
        vout.spending_txid = spending_inpoint.spending_txid;
        vout.spending_index = spending_inpoint.spending_index;
      }
      vout.coin = vout.coin;
      return vout;
    }
    let args: any[] = [transactionInput.txid, transactionInput.vout];
    let query: string = "SELECT * FROM "+transactionInput.coin.keyspace+".transaction_output WHERE txid=? AND n=?";
    let resultSet: types.ResultSet = await this.client.execute(
      query, 
      args, 
      {prepare: true}
    );
    let res: TransactionOutput[] = resultSet.rows.map(row => {
      let vout: TransactionOutput = new TransactionOutput();
      vout.txid = row.get('txid');
      vout.n = row.get('n');
      vout.value = row.get('value');
      let scriptpubkey = row.get('scriptpubkey');
      if (scriptpubkey.addresses !== undefined && scriptpubkey.addresses !== null) {
        scriptpubkey.addresses = scriptpubkey.addresses.map(address => new Address(address, transactionInput.coin));
      }
      vout.scriptpubkey = scriptpubkey;
      vout.spending_txid = row.get('spending_txid');
      vout.spending_index = row.get('spending_index');
      if (vout.spending_txid === undefined || vout.spending_txid === null) {
        let spending_inpoint = mempool === undefined ? undefined : mempool.outpointToInpoint.get(vout.txid+vout.n);
        if (spending_inpoint !== null) {
          vout.spending_txid = spending_inpoint.spending_txid;
          vout.spending_index = spending_inpoint.spending_index;
        }
      }

      vout.coin = vout.coin;
      return vout;
    });
    return res[0];
  }

  @FieldResolver( {complexity: ({ childComplexity, args }) => 100 + childComplexity})
  async transaction(@Root() transactionInput: TransactionInput, 
  ): Promise<Transaction> {
    if (transactionInput.spending_txid === null || transactionInput.spending_txid === undefined) return null;
    let mempool = transactionInput.coin.mempool;
    let mempoolTx = mempool === undefined ? undefined : mempool.txById.get(transactionInput.spending_txid);
    if (mempoolTx !== undefined) {
      let tx: Transaction = new Transaction();
      tx.txid = mempoolTx.txid;
      tx.locktime = mempoolTx.locktime;
      tx.size = mempoolTx.size;
      tx.version = mempoolTx.version;
      tx.height = mempoolTx.height;
      tx.txN = mempoolTx.txN;
      tx.fee = mempoolTx.fee;
      tx.coin = transactionInput.coin;
      return tx;
    }
    let args: any[] = [transactionInput.spending_txid];
    let query: string = "SELECT * FROM "+transactionInput.coin.keyspace+".transaction WHERE txid=?";
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
      tx.fee = row.get("fee");
      tx.coin = transactionInput.coin;
      return tx;
    });
    return res[0];
  }

}