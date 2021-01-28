import { types } from "cassandra-driver";
import { Arg, Args, ArgsType, Field, FieldResolver, Int, Query, Resolver, Root } from "type-graphql";
import { Inject } from "typedi";
import { LimitedCapacityClient } from "../limited-capacity-client";
import { MempoolBlock, MempoolTx } from "../mempool";
import { Address } from "../models/address";
import { AddressCluster } from "../models/address-cluster";
import { AddressClusterRichlist, AddressClusterRichlistCursor, PaginatedAddressClusterRichlistResponse } from "../models/address-cluster-richlist";
import { Block } from "../models/block";
import { BlockHash } from "../models/block_hash";
import { Coin } from "../models/coin";
import { ConfirmedTransaction } from "../models/confirmed-transaction";
import { Date as DateModel } from "../models/date";
import { Transaction } from "../models/transaction";
import { PaginationArgs } from "./pagination-args";

@ArgsType()
class ClusterRichlistArgs extends PaginationArgs {

  @Field({nullable: true})
  cursor: AddressClusterRichlistCursor;

}

@Resolver(of => Coin)
export class CoinResolver {

    constructor(
      @Inject("cassandra_client") private client: LimitedCapacityClient, 
      @Inject("coins_keyspace") private coins_keyspace: string, 
      @Inject("coins") private available_coins: Map<string, Coin>
    ) {
        this.coins();//updates lastCoinCount;
    }

    private static lastCoinCount: number = 1;

    public static CLUSTER_RICHLIST_BIN_COUNT: number = 100;
    static CLUSTER_RICHLIST_BINS: number[] = Array.from(new Array(CoinResolver.CLUSTER_RICHLIST_BIN_COUNT).keys());

    @Query(returns => [Coin], {nullable: true, complexity: ({ childComplexity, args }) => 100 + CoinResolver.lastCoinCount * childComplexity})
    async coins(): Promise<Coin[]> {
      CoinResolver.lastCoinCount = this.available_coins.size;
      return Array.from(this.available_coins.values());
    }

    @Query(returns => Coin, {nullable: true, complexity: 100})
    async coin(@Arg("name") name: string): Promise<Coin> {
      return this.available_coins.get(name);
    }

    @FieldResolver(returns => DateModel, {complexity: 1})
    async date(
        @Root() coin: Coin, 
        @Arg("date") date: string
    ): Promise<DateModel> {
        let res = new DateModel();
        res.date = date;
        res.coin = coin;
        return res;
    }

    @FieldResolver(returns => Address, {complexity: 1})
    async address(@Root() coin: Coin, @Arg("address") address: string) {
        let res = new Address(address, coin);
        return res;
    }

    @FieldResolver(returns => Transaction, {nullable: true, complexity: ({ childComplexity, args }) => 100 + childComplexity})
    async transaction(@Root() coin: Coin, @Arg("txid") txid: string): Promise<Transaction> {
      let mempool = coin.mempool;
      let mempoolTransaction = mempool === undefined ? undefined : mempool.txById.get(txid);
      if (mempoolTransaction !== undefined) {
        return mempoolTransaction.toGraphQL(coin);
      }
      let args: any[] = [txid];
      let query: string = "SELECT * FROM "+coin.keyspace+".transaction WHERE txid=?";
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
        tx.coin = coin;
        return tx;
      });
      return res[0];
  }

  @FieldResolver(returns => ConfirmedTransaction, {nullable: true, complexity: ({ childComplexity, args }) => 100 + childComplexity})
  async confirmedTransaction(
    @Root() coin: Coin,
    @Arg("height", type => Int) height: number, 
    @Arg("tx_n", type => Int) tx_n: number
  ): Promise<ConfirmedTransaction> {
    //let mempool: Mempool = this.mempools.get(coin.name);
    let mempool = coin.mempool;
    let mempoolBlock: MempoolBlock = mempool === undefined ? undefined : mempool.blockByHeight.get(height);
    if (mempoolBlock !== undefined) {
      let mempoolTx: MempoolTx = mempoolBlock.tx[tx_n];
      let tx: ConfirmedTransaction = new ConfirmedTransaction();
      tx.height = mempoolTx.height;
      tx.tx_n = mempoolTx.txN;
      tx.txid = mempoolTx.txid;
      tx.coin = coin;
      return tx;
    }
    let args: any[] = [height, tx_n];
    let query: string = "SELECT * FROM "+coin.keyspace+".confirmed_transaction WHERE height=? AND tx_n=?";
    let resultSet: types.ResultSet = await this.client.execute(
      query, 
      args, 
      {prepare: true}
    );
    let res: ConfirmedTransaction[] = resultSet.rows.map(row => {
      let tx: ConfirmedTransaction = new ConfirmedTransaction();
      tx.height = row.get('height');
      tx.tx_n = row.get('tx_n');
      tx.txid = row.get("txid");
      tx.coin = coin;
      return tx;
    });
    return res[0];
  }

  @FieldResolver(returns => BlockHash, {nullable: true, complexity: ({ childComplexity, args }) => 100 + childComplexity})
  async blockByHeight(
    @Root() coin: Coin,
    @Arg("height", type => Int) height: number
  ): Promise<BlockHash> {
    //let mempool: Mempool = this.mempools.get(coin.name);
    let mempool = coin.mempool;
    let mempoolBlock = mempool === undefined ? undefined : mempool.blockByHeight.get(height);
    if (mempoolBlock !== undefined) {
      let res: BlockHash = new BlockHash();
      res.hash = mempoolBlock.hash;
      res.height = mempoolBlock.height;
      res.coin = coin;
      return res;
    }
    let args: any[] = [height];
    let query: string = "SELECT * FROM "+coin.keyspace+".longest_chain WHERE height=?";
    let resultSet: types.ResultSet = await this.client.execute(
      query, 
      args, 
      {prepare: true}
    );
    let res: BlockHash[] = resultSet.rows.map(row => {
      let b: BlockHash = new BlockHash();
      b.hash = row.get('hash');
      b.height = row.get('height');
      b.coin = coin;
      return b;
    });
    return res[0];
  }    

  @FieldResolver(returns => Block, {nullable: true, complexity: ({ childComplexity, args }) => 100 + childComplexity})
  async block(
    @Root() coin: Coin,
    @Arg("hash") hash: string
  ): Promise<Block> {
    //let mempool: Mempool = this.mempools.get(coin.name);
    let mempool = coin.mempool;
    let mempooBlock: MempoolBlock = mempool === undefined ? undefined : mempool.blockByHash.get(hash);
    if (mempooBlock !== undefined) {
      let b: Block = new Block();
      b.height = mempooBlock.height;
      b.hash = mempooBlock.hash;
      b.size = mempooBlock.size;
      b.height = mempooBlock.height;
      b.version = mempooBlock.version;
      b.versionhex = mempooBlock.versionHex;
      b.merkleroot = mempooBlock.merkleroot;
      b.time = new Date(mempooBlock.time*1000);
      b.mediantime = mempooBlock.mediantime;
      b.nonce = mempooBlock.nonce;
      b.bits = mempooBlock.bits;
      b.difficulty = mempooBlock.difficulty;
      b.chainwork = mempooBlock.chainwork;
      b.previousblockhash = mempooBlock.previousblockhash;
      b.tx_count = mempooBlock.tx.length;
      b.coin = coin;
      return b;
    }
    let args: any[] = [hash];
    let query: string = "SELECT * FROM "+coin.keyspace+".block WHERE hash=?";
    let resultSet: types.ResultSet = await this.client.execute(
      query, 
      args, 
      {prepare: true}
    );
    let res: Block[] = resultSet.rows.map(row => {
      let b: Block = new Block();
      b.height = row.get('height');
      b.hash = row.get('hash');
      b.size = row.get("size");
      b.height = row.get('height');
      b.version = row.get('version');
      b.versionhex = row.get("versionhex");
      b.merkleroot = row.get("merkleroot");
      b.time = row.get("time");
      b.mediantime = row.get("mediantime");
      b.nonce = row.get("nonce");
      b.bits = row.get("bits");
      b.difficulty = row.get("difficulty");
      b.chainwork = row.get("chainwork");
      b.previousblockhash = row.get("previousblockhash");
      b.tx_count = row.get("tx_count");
      b.coin = coin;
      return b;
    });
    return res[0];
  }  

  @FieldResolver(returns => PaginatedAddressClusterRichlistResponse, {complexity: ({ childComplexity, args }) => 100 + args.limit * childComplexity})
  async clusterRichlist(@Root() coin: Coin, 
    @Args() {limit, cursor}: ClusterRichlistArgs
  ): Promise<PaginatedAddressClusterRichlistResponse> {
    let args: any[] = [CoinResolver.CLUSTER_RICHLIST_BINS];
    let query: string = "SELECT balance, cluster_id FROM "+coin.keyspace+".cluster_richlist WHERE bin IN ?";
    if (cursor) {
      query += " AND (balance, cluster_id) < (?, ?)";
      args = args.concat([cursor.balance, cursor.clusterId]);
    }
    args.push(limit+1); 
    query += " ORDER BY balance DESC LIMIT ?";
    let resultSet: types.ResultSet = await this.client.execute(
      query, 
      args, 
      {prepare: true, fetchSize: null}
    );
    let hasMore: boolean = resultSet.rows.length > limit;
    if (hasMore) resultSet.rows.pop();
    let res: AddressClusterRichlist[] = resultSet.rows.map(row => {
      let e = new AddressClusterRichlist();
      e.balance = row.get("balance");
      e.cluster = new AddressCluster();
      e.cluster.coin = coin;
      e.cluster.clusterId = row.get("cluster_id");
      return e;
    });
    return {
      hasMore: hasMore,
      items: res,
    };
  }
}