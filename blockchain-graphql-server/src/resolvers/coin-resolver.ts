import { types } from "cassandra-driver";
import { Arg, Args, ArgsType, Field, FieldResolver, Int, Query, Resolver, Root } from "type-graphql";
import { Inject } from "typedi";
import { LimitedCapacityClient } from "../limited-capacity-client";
import { MempoolBlock, MempoolTx } from "../mempool/mempool";
import { Address } from "../models/address";
import { AddressCluster } from "../models/address-cluster";
import { AddressClusterRichlist, AddressClusterRichlistCursor, PaginatedAddressClusterRichlistResponse } from "../models/address-cluster-richlist";
import { Block } from "../models/block";
import { BlockHash, BlockHashCursor, PaginatedAddressBlockHashtResponse } from "../models/block_hash";
import { Coin } from "../models/coin";
import { ConfirmedTransaction } from "../models/confirmed-transaction";
import { Date as DateModel } from "../models/date";
import { MempoolModel } from "../models/mempool-model";
import { Transaction } from "../models/transaction";
import { PaginationArgs } from "./pagination-args";

@ArgsType()
class ClusterRichlistArgs extends PaginationArgs {

  @Field({nullable: true})
  cursor: AddressClusterRichlistCursor;

}

@ArgsType()
class BlockHashArgs extends PaginationArgs {

  @Field({nullable: true})
  cursor: BlockHashCursor;

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

    @Query(returns => [Coin], {nullable: false, complexity: ({ childComplexity, args }) => 100 + CoinResolver.lastCoinCount * childComplexity})
    async coins(): Promise<Coin[]> {
      CoinResolver.lastCoinCount = this.available_coins.size;
      return Array.from(this.available_coins.values());
    }

    @Query(returns => Coin, {nullable: true, complexity: 100})
    async coin(@Arg("name") name: string): Promise<Coin> {
      return this.available_coins.get(name);
    }

    @FieldResolver(returns => DateModel, {nullable: false, complexity: 1})
    async date(
        @Root() coin: Coin, 
        @Arg("date") date: string
    ): Promise<DateModel> {
      return <DateModel> {
        date: date,
        coin: coin
      }
    }

    @FieldResolver(returns => MempoolModel, {nullable: false, complexity: 1})
    async mempool(
        @Root() coin: Coin
    ): Promise<MempoolModel> {
      return <MempoolModel> {coin: coin};
    }

    @FieldResolver(returns => Address, {nullable: false, complexity: 1})
    async address(@Root() coin: Coin, @Arg("address") address: string) {
        let res = new Address({address: address, coin: coin});
        return res;
    }

    @FieldResolver(returns => Transaction, {nullable: true, complexity: ({ childComplexity, args }) => 100 + childComplexity})
    async transaction(@Root() coin: Coin, @Arg("txid") txid: string): Promise<Transaction> {
      let mempoolTransaction = coin.mempool?.txById.get(txid);
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
        return <Transaction> {
          txid: row.get('txid'),
          lockTime: row.get('locktime'),
          size: row.get('size'),
          version: row.get('version'),
          height: row.get('height'),
          txN: row.get("tx_n"),
          fee: row.get("fee"),
          coin: coin
        }
      });
      return res[0];
  }

  @FieldResolver(returns => ConfirmedTransaction, {nullable: true, complexity: ({ childComplexity, args }) => 100 + childComplexity})
  async confirmedTransaction(
    @Root() coin: Coin,
    @Arg("height", type => Int) height: number, 
    @Arg("tx_n", type => Int) tx_n: number
  ): Promise<ConfirmedTransaction> {
    let mempoolBlock: MempoolBlock = coin.mempool?.blockByHeight.get(height);
    if (mempoolBlock !== undefined) {
      let mempoolTx: MempoolTx = mempoolBlock.tx[tx_n];
      return <ConfirmedTransaction> {
        height: mempoolTx.height,
        txN: mempoolTx.txN,
        txid: mempoolTx.rpcTx.txid,
        coin: coin
      };
    }
    let args: any[] = [height, tx_n];
    let query: string = "SELECT * FROM "+coin.keyspace+".confirmed_transaction WHERE height=? AND tx_n=?";
    let resultSet: types.ResultSet = await this.client.execute(
      query, 
      args, 
      {prepare: true}
    );
    let res: ConfirmedTransaction[] = resultSet.rows.map(row => {
      return <ConfirmedTransaction> {
        height: row.get('height'),
        txN: row.get('tx_n'),
        txid: row.get("txid"),
        coin: coin
      };
    });
    return res[0];
  }

  @FieldResolver(returns => PaginatedAddressBlockHashtResponse, {nullable: false, complexity: ({ childComplexity, args }) => args.limit * childComplexity})
  async blocks(
    @Root() coin: Coin,
    @Args() {limit, cursor}: BlockHashArgs
  ): Promise<PaginatedAddressBlockHashtResponse> {
    let lastBlockHeight: number = coin.mempool?.height;
    if (lastBlockHeight === undefined) {
      return {hasMore: false, items: []};
    }
    let fromHeight = (cursor?.height === undefined || cursor?.height === null) ? lastBlockHeight : Math.min(cursor.height-1, lastBlockHeight);
    let res: Promise<BlockHash>[] = [];
    for (let currentHeight: number = fromHeight; currentHeight >= Math.max(fromHeight-limit+1, 0); currentHeight--) {
      res.push(this.blockByHeight(coin, currentHeight));
    }
    return new Promise<PaginatedAddressBlockHashtResponse>(async (resolve, reject) => {
      resolve({
        hasMore: fromHeight-limit+1 > 0,
        items: await Promise.all(res)
      })
    });
  } 

  @FieldResolver(returns => BlockHash, {nullable: true, complexity: ({ childComplexity, args }) => 100 + childComplexity})
  async blockByHeight(
    @Root() coin: Coin,
    @Arg("height", type => Int) height: number
  ): Promise<BlockHash> {
    let mempoolBlock: MempoolBlock = coin.mempool?.blockByHeight.get(height);
    if (mempoolBlock !== undefined) {
      return <BlockHash> {
        hash: mempoolBlock.rpcBlock.hash,
        height: mempoolBlock.rpcBlock.height,
        coin: coin
      }
    }
    let args: any[] = [height];
    let query: string = "SELECT * FROM "+coin.keyspace+".longest_chain WHERE height=?";
    let resultSet: types.ResultSet = await this.client.execute(
      query, 
      args, 
      {prepare: true}
    );
    let res: BlockHash[] = resultSet.rows.map(row => {
      return <BlockHash> {
        hash: row.get('hash'),
        height: row.get('height'),
        coin: coin
      }
    });
    return res[0];
  }    

  @FieldResolver(returns => Block, {nullable: true, complexity: ({ childComplexity, args }) => 100 + childComplexity})
  async block(
    @Root() coin: Coin,
    @Arg("hash") hash: string
  ): Promise<Block> {
    let mempooBlock: MempoolBlock = coin.mempool?.blockByHash.get(hash);
    if (mempooBlock !== undefined) {
      return <Block> {
        height: mempooBlock.rpcBlock.height,
        hash: mempooBlock.rpcBlock.hash,
        size: mempooBlock.rpcBlock.size,
        version: mempooBlock.rpcBlock.version,
        versionHex: mempooBlock.rpcBlock.versionHex,
        merkleRoot: mempooBlock.rpcBlock.merkleroot,
        time: new Date(mempooBlock.rpcBlock.time*1000),
        medianTime: mempooBlock.rpcBlock.mediantime,
        nonce: mempooBlock.rpcBlock.nonce,
        bits: mempooBlock.rpcBlock.bits,
        difficulty: mempooBlock.rpcBlock.difficulty,
        chainwork: mempooBlock.rpcBlock.chainwork,
        previousBlockHash: mempooBlock.rpcBlock.previousblockhash,
        txCount: mempooBlock.tx.length,
        coin: coin
      }
    }
    let args: any[] = [hash];
    let query: string = "SELECT * FROM "+coin.keyspace+".block WHERE hash=?";
    let resultSet: types.ResultSet = await this.client.execute(
      query, 
      args, 
      {prepare: true}
    );
    let res: Block[] = resultSet.rows.map(row => {
      return <Block> {
        height: row.get('height'),
        hash: row.get('hash'),
        size: row.get("size"),
        version: row.get('version'),
        versionHex: row.get("versionhex"),
        merkleRoot: row.get("merkleroot"),
        time: row.get("time"),
        medianTime: row.get("mediantime"),
        nonce: row.get("nonce"),
        bits: row.get("bits"),
        difficulty: row.get("difficulty"),
        chainwork: row.get("chainwork"),
        previousBlockHash: row.get("previousblockhash"),
        txCount: row.get("tx_count"),
        coin: coin
      }
    });
    return res[0];
  }  

  @FieldResolver(returns => PaginatedAddressClusterRichlistResponse, {nullable: false, complexity: ({ childComplexity, args }) => 100 + args.limit * childComplexity})
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
    query += " ORDER BY balance DESC, cluster_id DESC LIMIT ?";
    let resultSet: types.ResultSet = await this.client.execute(
      query, 
      args, 
      {prepare: true, fetchSize: null}
    );
    let hasMore: boolean = resultSet.rows.length > limit;
    if (hasMore) resultSet.rows.pop();
    let res: AddressClusterRichlist[] = resultSet.rows.map(row => {
      return <AddressClusterRichlist> {
        balance: row.get("balance"),
        cluster: <AddressCluster> {
          coin: coin,
          clusterId: row.get("cluster_id")
        }
      }
    });
    return {
      hasMore: hasMore,
      items: res
    };
  }
}