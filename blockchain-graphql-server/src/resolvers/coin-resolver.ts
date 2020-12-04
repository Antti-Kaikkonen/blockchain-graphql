import { Client, types } from "cassandra-driver";
import { Arg, FieldResolver, Query, Resolver, Root } from "type-graphql";
import { Inject } from "typedi";
import { Address } from "../models/address";
import { Block } from "../models/block";
import { BlockHash } from "../models/block_hash";
import { Coin } from "../models/coin";
import { ConfirmedTransaction } from "../models/confirmed-transaction";
import { Date } from "../models/date";
import { Transaction } from "../models/transaction";

@Resolver(of => Coin)
export class CoinResolver {

    constructor(@Inject("cassandra_client") private client: Client, @Inject("coins_keyspace") private coins_keyspace: string) {
        this.coins();//updates lastCoinCount;
    }

    private static lastCoinCount: number = 1;

    @Query(returns => [Coin], {nullable: true, complexity: ({ childComplexity, args }) => 100 + CoinResolver.lastCoinCount * childComplexity})
    async coins(): Promise<Coin[]> {
        let args: any[] = [];
        let query: string = "SELECT * FROM "+this.coins_keyspace+".available_coins";
        let resultSet: types.ResultSet = await this.client.execute(
            query, 
            args, 
            {prepare: true}
        );
        let res: Coin[] = resultSet.rows.map(row => {
            let coin: Coin = new Coin();
            coin.name = row.get('name');
            coin.keyspace = row.get('key_space');
            return coin;
        });
        CoinResolver.lastCoinCount = res.length;
        return res;
    }

    @Query(returns => Coin, {nullable: true, complexity: 100})
    async coin(@Arg("name") name: string): Promise<Coin> {
        let args: any[] = [name];
        let query: string = "SELECT * FROM coins.available_coins WHERE name=?";
        let resultSet: types.ResultSet = await this.client.execute(
            query, 
            args, 
            {prepare: true}
        );
        let res: Coin[] = resultSet.rows.map(row => {
            let coin: Coin = new Coin();
            coin.name = row.get('name');
            coin.keyspace = row.get('key_space');
            return coin;
        });
        return res[0];
    }

    @FieldResolver(returns => Date, {complexity: 1})
    async date(
        @Root() coin: Coin, 
        @Arg("date") date: string
    ): Promise<Date> {
        let res = new Date();
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
      tx.fee = row.get("tx_fee")/1e8;
      tx.coin = coin;
      return tx;
    });
    return res[0];
  }

  @FieldResolver(returns => ConfirmedTransaction, {nullable: true, complexity: ({ childComplexity, args }) => 100 + childComplexity})
  async confirmedTransaction(
    @Root() coin: Coin,
    @Arg("height") height: number, 
    @Arg("tx_n") tx_n: number): Promise<ConfirmedTransaction> {
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
  async blockHash(
    @Root() coin: Coin,
    @Arg("height") height: number): Promise<BlockHash> {
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
    @Arg("hash") hash: string): Promise<Block> {
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
}