package io.github.anttikaikkonen.blockchainanalyticsflink.casssandra;

public class CreateStatements {
    public static final String TABLE_ADDRESS_BALANCE = "CREATE TABLE IF NOT EXISTS address_balance (\n" +
"        address text,\n" +
"        balance double,\n" +
"        timestamp timestamp,\n" +
"        PRIMARY KEY(address, timestamp)\n" +
")\n" +
"WITH \n" +
"	CLUSTERING ORDER BY (timestamp DESC)\n" +
"AND\n" +
"	COMPRESSION = {'sstable_compression': 'LZ4Compressor', 'chunk_length_in_kb': 16};";
    
    public static final String TABLE_ADDRESS_TRANSACTION = "CREATE TABLE IF NOT EXISTS address_transaction (\n" +
"    address text,\n" +
"    timestamp timestamp,\n" +
"    balance_change double,\n" +
"    height int,\n" +
"    tx_n int,\n" +
"    PRIMARY KEY(address, timestamp, height, tx_n)\n" +
")\n" +
"WITH \n" +
"	CLUSTERING ORDER BY (timestamp DESC, height DESC, tx_n DESC)\n" +
"AND\n" +
"	COMPRESSION = {'sstable_compression': 'LZ4Compressor', 'chunk_length_in_kb': 16};";
    
    public static final String TABLE_BLOCK = "CREATE TABLE IF NOT EXISTS block (\n" +
"    hash text,\n" +
"    height int,\n" +
"    version int,\n" +
"    versionhex text,\n" +
"    merkleroot text,\n" +
"    time bigint,\n" +
"    mediantime bigint,\n" +
"    nonce bigint,\n" +
"    bits text,\n" +
"    difficulty decimal,\n" +
"    chainwork text,\n" +
"    previousblockhash text,\n" +
"    size int,\n" +
"    tx_count int,\n" +
"    PRIMARY KEY(hash)\n" +
")\n" +
"WITH COMPRESSION = {'sstable_compression': 'LZ4Compressor', 'chunk_length_in_kb': 4};";
    
    
    public static final String TABLE_CONFIRMED_TRANSACTION = "CREATE TABLE IF NOT EXISTS confirmed_transaction (\n" +
"    height int,\n" +
"    tx_n int,\n" +
"    txid text,\n" +
"    PRIMARY KEY(height, tx_n)\n" +
")\n" +
"WITH COMPRESSION = {'sstable_compression': 'LZ4Compressor', 'chunk_length_in_kb': 16};";
    
    public static final String TABLE_LONGEST_CHAIN = "CREATE TABLE IF NOT EXISTS longest_chain (\n" +
"	height int,\n" +
"	hash text,\n" +
"	PRIMARY KEY(height)\n" +
")\n" +
"WITH COMPRESSION = {'sstable_compression': 'LZ4Compressor', 'chunk_length_in_kb': 4};";
    
    public static final String TABLE_OHLC = "CREATE TABLE IF NOT EXISTS ohlc (\n" +
"	address text,\n" +
"	open double,\n" +
"	high double,\n" +
"	low double,\n" +
"	close double,\n" +
"	timestamp timestamp,\n" +
"	interval int,\n" +
"	PRIMARY KEY((address, interval), timestamp)\n" +
")\n" +
"WITH COMPRESSION = {'sstable_compression': 'LZ4Compressor', 'chunk_length_in_kb': 16};";
    
    public static final String TABLE_DAILY_RICHLIST = "CREATE TABLE IF NOT EXISTS daily_richlist (\n" +
"	address text,\n" +
"	balance double,\n" +
"	balance_change double,\n" +
"	date date,\n" +
"	bin tinyint,\n" +
"	PRIMARY KEY((date, bin), balance, balance_change, address)\n" +
")\n" +
"WITH \n" +
"	CLUSTERING ORDER BY (balance DESC, balance_change DESC, address DESC)\n" +
"AND\n" +
"	COMPRESSION = {'sstable_compression': 'LZ4Compressor', 'chunk_length_in_kb': 16};";
    
    public static final String TABLE_DAILY_TOP_GAINERS = "CREATE TABLE IF NOT EXISTS daily_top_gainers (\n" +
"        address text,\n" +
"        balance_change double,\n" +
"        date date,\n" +
"        bin tinyint,\n" +
"        PRIMARY KEY((date, bin), balance_change, address)\n" +
")\n" +
"WITH \n" +
"	CLUSTERING ORDER BY (balance_change DESC, address DESC)\n" +
"AND\n" +
"	COMPRESSION = {'sstable_compression': 'LZ4Compressor', 'chunk_length_in_kb': 16};";
    
    public static final String TABLE_DAILY_TOP_LOSERS = "CREATE TABLE IF NOT EXISTS daily_top_losers (\n" +
"        address text,\n" +
"        balance_change double,\n" +
"        date date,\n" +
"        bin tinyint,\n" +
"        PRIMARY KEY((date, bin), balance_change, address)\n" +
")\n" +
"WITH COMPRESSION = {'sstable_compression': 'LZ4Compressor', 'chunk_length_in_kb': 16};";
    
    public static final String TABLE_TRANSACTION = "CREATE TABLE IF NOT EXISTS transaction (\n" +
"    height int,\n" +
"    tx_n int,\n" +
"    txid text,\n" +
"    size int,\n" +
"    version bigint,\n" +
"    locktime bigint,\n" +
"    input_count int,\n" +
"    output_count int,\n" +
"    fee double,\n" +
"    PRIMARY KEY(txid)\n" +
")\n" +
"WITH COMPRESSION = {'sstable_compression': 'LZ4Compressor', 'chunk_length_in_kb': 4};";
    
    public static final String TYPE_SCRIPTSIG = "CREATE TYPE IF NOT EXISTS ScriptSig (\n" +
"  asm text,\n" +
"  hex text\n" +
");";
    
    public static final String TABLE_TRANSACTION_INPUT = "CREATE TABLE IF NOT EXISTS transaction_input (\n" +
"    spending_txid text,\n" +
"    spending_index int,\n" +
"    txid text,\n" +
"    vout int,\n" +
"    coinbase text,\n" +
"    sequence bigint,\n" +
"    scriptSig frozen<ScriptSig>,\n" +
"    PRIMARY KEY(spending_txid, spending_index)\n" +
")\n" +
"WITH COMPRESSION = {'sstable_compression': 'LZ4Compressor', 'chunk_length_in_kb': 16};";
    
    public static final String TYPE_SCRIPTPUBKEY = "CREATE TYPE IF NOT EXISTS ScriptPubKey (\n" +
"  asm text,\n" +
"  hex text,\n" +
"  reqSigs int,\n" +
"  type text,\n" +
"  addresses frozen<list<text>>,\n" +
");";
    
    public static final String TABLE_TRANSACTION_OUTPUT = "CREATE TABLE IF NOT EXISTS transaction_output (\n" +
"  txid text,\n" +
"  n int,\n" +
"  value double,\n" +
"  scriptpubkey frozen<ScriptPubKey>,\n" +
"  spending_txid text,\n" +
"  spending_index int,\n" +
"  PRIMARY KEY(txid, n)\n" +
")\n" +
"WITH COMPRESSION = {'sstable_compression': 'LZ4Compressor', 'chunk_length_in_kb': 16};";
    
    public static final String TABLE_UNION_FIND = "CREATE TABLE IF NOT EXISTS union_find (\n" +
"  address text,\n" +
"  parent text,\n" +
"  PRIMARY KEY(address)\n" +
")\n" +
"WITH COMPRESSION = {'sstable_compression': 'LZ4Compressor', 'chunk_length_in_kb': 4};";
    
    public static final String TABLE_CLUSTER_ADDRESS = "CREATE TABLE IF NOT EXISTS cluster_address (\n" +
"  cluster_id text,\n" +
"  address text,\n" +
"  PRIMARY KEY(cluster_id, address)\n" +
")\n" +
"WITH COMPRESSION = {'sstable_compression': 'LZ4Compressor', 'chunk_length_in_kb': 16};";
    
    public static final String TABLE_CLUSTER_TRANSACTION = "CREATE TABLE IF NOT EXISTS cluster_transaction (\n" +
"  cluster_id text,\n" +
"  timestamp timestamp,\n" +
"  height int,\n" +
"  tx_n int,\n" +
"  balance_change double,\n" +
"  PRIMARY KEY(cluster_id, timestamp, height, tx_n)\n" +
")\n" +
"WITH \n" +
"	CLUSTERING ORDER BY (timestamp DESC, height DESC, tx_n DESC)\n" +
"AND\n" +
"	COMPRESSION = {'sstable_compression': 'LZ4Compressor', 'chunk_length_in_kb': 16};";
    
}
