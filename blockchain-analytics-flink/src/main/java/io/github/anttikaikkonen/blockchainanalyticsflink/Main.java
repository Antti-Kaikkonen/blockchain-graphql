package io.github.anttikaikkonen.blockchainanalyticsflink;


import io.github.anttikaikkonen.blockchainanalyticsflink.source.HeaderTimeProcessor;
import io.github.anttikaikkonen.blockchainanalyticsflink.source.AsyncBlockHashFetcher;
import io.github.anttikaikkonen.blockchainanalyticsflink.source.AsyncBlockFetcher;
import io.github.anttikaikkonen.blockchainanalyticsflink.source.AsyncBlockHeadersFetcher;
import io.github.anttikaikkonen.blockchainanalyticsflink.source.BlockHeightSource;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.WriteType;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.policies.RetryPolicy;
import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.BlockSink;
import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.CassandraSessionBuilder;
import io.github.anttikaikkonen.blockchainanalyticsflink.models.ConfirmedTransaction;
import io.github.anttikaikkonen.blockchainanalyticsflink.models.ConfirmedTransactionWithInputs;
import io.github.anttikaikkonen.blockchainanalyticsflink.models.TransactionInputWithOutput;
import io.github.anttikaikkonen.blockchainanalyticsflink.statefun.unionfind.UnionFindFunction;
import io.github.anttikaikkonen.bitcoinrpcclientjava.LeastConnectionsRpcClient;
import io.github.anttikaikkonen.bitcoinrpcclientjava.LimitedCapacityRpcClient;
import io.github.anttikaikkonen.bitcoinrpcclientjava.RpcClient;
import io.github.anttikaikkonen.bitcoinrpcclientjava.models.Block;
import io.github.anttikaikkonen.bitcoinrpcclientjava.models.BlockHeader;
import io.github.anttikaikkonen.bitcoinrpcclientjava.models.Transaction;
import io.github.anttikaikkonen.bitcoinrpcclientjava.models.TransactionOutput;
import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.AddressSink;
import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.CreateStatements;
import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.TransactionSink;
import io.github.anttikaikkonen.blockchainanalyticsflink.models.InputPointer;
import io.github.anttikaikkonen.blockchainanalyticsflink.models.SpentOutput;
import io.github.anttikaikkonen.blockchainanalyticsflink.precluster.BlockClusterProcessor;
import io.github.anttikaikkonen.blockchainanalyticsflink.precluster.BlockClustering;
import io.github.anttikaikkonen.blockchainanalyticsflink.precluster.ConfirmedTransactionToDisjointSets;
import io.github.anttikaikkonen.blockchainanalyticsflink.precluster.SimpleAddAddressesAndTransactionsOperation;
import io.github.anttikaikkonen.blockchainanalyticsflink.statefun.sink.UnionFindSink;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.statefun.flink.core.StatefulFunctionsConfig;
import org.apache.flink.statefun.flink.core.message.MessageFactoryType;
import org.apache.flink.statefun.flink.core.message.RoutableMessage;
import org.apache.flink.statefun.flink.datastream.StatefulFunctionDataStreamBuilder;
import org.apache.flink.statefun.flink.datastream.StatefulFunctionEgressStreams;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.reactor.IOReactorConfig;

public class Main {
    
    
    private static final int BLOCK_FETCHER_CONCURRENCY = 1;
    public static final int CASSANDRA_CONCURRENT_REQUESTS = 100;
    
    public static final String PROPERTIES_CASSANDRA_HOST = "cassandra.host";
    public static final String PROPERTIES_CASSANDRA_KEYSPACE = "cassandra.keyspace";
    public static final String PROPERTIES_BLOCKCHAIN_RPC_URL = "blockchain.rpc.url";
    public static final String PROPERTIES_BLOCKCHAIN_RPC_USERNAME = "blockchain.rpc.username";
    public static final String PROPERTIES_BLOCKCHAIN_RPC_PASSWORD = "blockchain.rpc.password";
    public static final String PROPERTIES_CONCURRENT_BLOCKS = "concurrent_blocks";
    public static final String PROPERTIES_TEST_MODE = "test_mode";
    public static final String PROPERTIES_DISABLE_SINKS = "disable_sinks";
    
    
    public static void main(String[] args) throws Exception {
        ParameterTool properties = ParameterTool.fromArgs(args);
        boolean test_mode = properties.getBoolean(PROPERTIES_TEST_MODE, false);
        boolean disable_sinks;
        final String[] cassandraHosts;
        final String cassandraKeyspace;
        final String[] blockchainRpcURLS;
        final String blockchainUsername;
        final String blockchainPassword;
        final int concurrentBlocks;
        if (test_mode) {
            cassandraHosts = null;
            cassandraKeyspace = null;
            blockchainRpcURLS = null;
            blockchainUsername = null;
            blockchainPassword = null;
            concurrentBlocks = -1;
            disable_sinks=true;
        } else {
            String propertiesFile = properties.get("properties-file");
            if (propertiesFile != null) {
                ParameterTool fileProperties = ParameterTool.fromPropertiesFile(propertiesFile);
                properties = fileProperties.mergeWith(properties);//arguments overwrite file properties
            }
            disable_sinks = properties.getBoolean(PROPERTIES_DISABLE_SINKS, false);
            System.out.println("Properties="+properties.toMap().toString());

            String cassandraHost = properties.get(PROPERTIES_CASSANDRA_HOST, "localhost");
            cassandraHosts = cassandraHost.split("\\s+");


            cassandraKeyspace = properties.get(PROPERTIES_CASSANDRA_KEYSPACE, "bitcoin");

            String blockchainRpcURL = properties.get(PROPERTIES_BLOCKCHAIN_RPC_URL, "http://localhost:8332");
            blockchainRpcURLS = blockchainRpcURL.split("\\s+");
            blockchainUsername = properties.getRequired(PROPERTIES_BLOCKCHAIN_RPC_USERNAME);
            blockchainPassword = properties.getRequired(PROPERTIES_BLOCKCHAIN_RPC_PASSWORD);

            concurrentBlocks = properties.getInt(PROPERTIES_CONCURRENT_BLOCKS, 200);
        }
        
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(60000*10, CheckpointingMode.EXACTLY_ONCE);
        env.setRestartStrategy(RestartStrategies.failureRateRestart(5, Time.of(3, TimeUnit.MINUTES), Time.of(10, TimeUnit.SECONDS)));//Allow 5 restarts within 3 minutes
        env.getConfig().disableAutoGeneratedUIDs();
        env.getConfig().enableObjectReuse();
        
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setMinPauseBetweenCheckpoints(60000*10);
        checkpointConfig.setCheckpointTimeout(Long.MAX_VALUE);//Checkpoints can take hours if they are triggered when flink is restoring a large state
        checkpointConfig.enableUnalignedCheckpoints(false);
        checkpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        
        CassandraSessionBuilder sessionBuilder = new CassandraSessionBuilder() {
            @Override
            protected Session createSession(Cluster.Builder builder) {
                Cluster cluster = builder
                        .addContactPoints(cassandraHosts)
                        .withSocketOptions(
                                new SocketOptions().setReadTimeoutMillis(120000)
                        )
                        .withPoolingOptions(
                                new PoolingOptions()
                                        .setConnectionsPerHost(HostDistance.LOCAL, 1, 1)
                                        .setConnectionsPerHost(HostDistance.REMOTE, 1, 1)
                                        .setMaxRequestsPerConnection(HostDistance.LOCAL, 30000)
                                        .setMaxRequestsPerConnection(HostDistance.REMOTE, 30000)
                                        .setMaxQueueSize(0)
                        ).withRetryPolicy(new RetryPolicy() {
                            @Override
                            public RetryPolicy.RetryDecision onReadTimeout(Statement statement, ConsistencyLevel cl, int requiredResponses, int receivedResponses, boolean dataRetrieved, int nbRetry) {
                                System.out.println("onReadTimeout "+nbRetry);
                                if (nbRetry < 5) {
                                    try {
                                        Thread.sleep((nbRetry+1)*1000);
                                    } catch (InterruptedException ex) {
                                        Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
                                    }
                                    return RetryPolicy.RetryDecision.retry(cl);
                                } else {
                                    return RetryPolicy.RetryDecision.rethrow();
                                }
                            }

                            @Override
                            public RetryPolicy.RetryDecision onWriteTimeout(Statement statement, ConsistencyLevel cl, WriteType arg2, int requiredAcks, int receivedAcks, int nbRetry) {
                                System.out.println("onWriteTimeout "+nbRetry);
                                if (nbRetry < 5) {
                                    try {
                                        Thread.sleep((nbRetry+1)*1000);
                                    } catch (InterruptedException ex) {
                                        Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
                                    }
                                    return RetryPolicy.RetryDecision.retry(cl);
                                } else {
                                    return RetryPolicy.RetryDecision.rethrow();
                                }
                            }

                            @Override
                            public RetryPolicy.RetryDecision onUnavailable(Statement statement, ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry) {
                                System.out.println("onUnavailable "+nbRetry);
                                if (nbRetry < 5) {
                                    try {
                                        Thread.sleep((nbRetry+1)*1000);
                                    } catch (InterruptedException ex) {
                                        Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
                                    }
                                    return RetryPolicy.RetryDecision.retry(cl);
                                } else {
                                    return RetryPolicy.RetryDecision.rethrow();
                                }
                            }

                            @Override
                            public RetryPolicy.RetryDecision onRequestError(Statement statement, ConsistencyLevel cl, DriverException driverException, int nbRetry) {
                                System.out.println("onRequestError "+nbRetry+", ex:"+driverException);
                                if (nbRetry < 5) {
                                    try {
                                        Thread.sleep((nbRetry+1)*1000);
                                    } catch (InterruptedException ex) {
                                        Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
                                    }
                                    return RetryPolicy.RetryDecision.retry(cl);
                                } else {
                                    return RetryPolicy.RetryDecision.rethrow();
                                }
                                //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
                            }

                            @Override
                            public void init(Cluster arg0) {
                            }

                            @Override
                            public void close() {
                            }
                        }).build();
                Session session = cluster.connect(cassandraKeyspace);
                return session;
            }
        };
        
        //Create schema if it doesn't already exist
        if (!test_mode && !disable_sinks) {
            Cluster cluster = Cluster.builder().addContactPoints(cassandraHosts).build();
            Session session = cluster.connect();
            session.execute("CREATE KEYSPACE IF NOT EXISTS "+cassandraKeyspace+" WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}");
            session.execute("USE "+cassandraKeyspace);
            session.execute(CreateStatements.TABLE_BLOCK);
            session.execute(CreateStatements.TABLE_LONGEST_CHAIN);

            session.execute(CreateStatements.TYPE_SCRIPTPUBKEY);
            session.execute(CreateStatements.TYPE_SCRIPTSIG);
            session.execute(CreateStatements.TABLE_CONFIRMED_TRANSACTION);
            session.execute(CreateStatements.TABLE_TRANSACTION);
            session.execute(CreateStatements.TABLE_TRANSACTION_OUTPUT);
            session.execute(CreateStatements.TABLE_TRANSACTION_INPUT);
            session.execute(CreateStatements.TABLE_ADDRESS_TRANSACTION);

            session.execute(CreateStatements.TABLE_OHLC);
            session.execute(CreateStatements.TABLE_DAILY_TOP_GAINERS);
            session.execute(CreateStatements.TABLE_DAILY_TOP_LOSERS);
            session.execute(CreateStatements.TABLE_DAILY_RICHLIST);
            session.execute(CreateStatements.TABLE_ADDRESS_BALANCE);

            session.execute(CreateStatements.TABLE_UNION_FIND);
            session.execute(CreateStatements.TABLE_CLUSTER_ADDRESS);
            session.execute(CreateStatements.TABLE_CLUSTER_TRANSACTION);
            session.close();
            session.getCluster().close();
        }
        //End of schema creation
        
        int parallelism = env.getParallelism();
        RpcClientBuilder rpcClientBuilder1 = new RpcClientBuilder() {
            @Override
            public RpcClient build() {
                CredentialsProvider provider = new BasicCredentialsProvider();
                provider.setCredentials(
                        AuthScope.ANY,
                        new UsernamePasswordCredentials(blockchainUsername, blockchainPassword)
                );


                IOReactorConfig ioReactorConfig = IOReactorConfig.custom()
                        .setIoThreadCount(1)
                        .build();

                CloseableHttpAsyncClient httpClient = HttpAsyncClients.custom()
                        .setDefaultIOReactorConfig(ioReactorConfig)
                        .setDefaultCredentialsProvider(provider)
                        .setMaxConnPerRoute(parallelism)
                        .setMaxConnTotal(parallelism)
                        .build();
                httpClient.start();
                
                RpcClient[] clients =  new RpcClient[blockchainRpcURLS.length];
                for (int i = 0; i < clients.length; i++) {
                    clients[i] = new LimitedCapacityRpcClient(httpClient, blockchainRpcURLS[i].trim(), parallelism);
                }
                return new LeastConnectionsRpcClient(clients);
            }
        };

        RpcClientBuilder rpcClientBuilder2 = new RpcClientBuilder() {
            @Override
            public RpcClient build() {
                CredentialsProvider provider = new BasicCredentialsProvider();
                provider.setCredentials(
                        AuthScope.ANY,
                        new UsernamePasswordCredentials(blockchainUsername, blockchainPassword)
                );


                IOReactorConfig ioReactorConfig = IOReactorConfig.custom()
                        .setIoThreadCount(1)
                        .build();

                CloseableHttpAsyncClient httpClient = HttpAsyncClients.custom()
                        .setDefaultIOReactorConfig(ioReactorConfig)
                        .setDefaultCredentialsProvider(provider)
                        .setMaxConnPerRoute(1)
                        .setMaxConnTotal(1)
                        .build();
                httpClient.start();
                
                RpcClient[] clients =  new RpcClient[blockchainRpcURLS.length];
                for (int i = 0; i < clients.length; i++) {
                    clients[i] = new LimitedCapacityRpcClient(httpClient, blockchainRpcURLS[i].trim(), 100);
                }
                return new LeastConnectionsRpcClient(clients);
            }
        };

        SingleOutputStreamOperator<Integer> blockHeights;
        if (test_mode) {
            blockHeights = env.addSource(new SourceFunction<Integer>() {
                @Override
                public void run(SourceContext<Integer> ctx) throws Exception {
                }

                @Override
                public void cancel() {
                }
            }).uid("block_height_source").name("Block height source");
        } else {
            BlockHeightSource blockHeightSource = BlockHeightSource.builder()
                    .minConfirmations(5)
                    .rpcClientBuilder(rpcClientBuilder1)
                    .sessionBuilder(disable_sinks ? null : sessionBuilder)
                    .concurrentBlocks(concurrentBlocks)
                    .build();
            blockHeights = env.addSource(blockHeightSource).uid("block_height_source").name("Block height source");
        }
        
        SingleOutputStreamOperator<String> blockHashes;
        if (test_mode) {
            blockHashes = blockHeights.map(e -> (String)null).uid("async_block_hash_fetcher").name("Block hash fetcher").forceNonParallel();
        } else {
            blockHashes = AsyncDataStream.orderedWait(
                blockHeights,
                new AsyncBlockHashFetcher(rpcClientBuilder1),
                60, 
                TimeUnit.SECONDS, 
                BLOCK_FETCHER_CONCURRENCY*env.getParallelism()
            ).uid("async_block_hash_fetcher").name("Block hash fetcher").forceNonParallel();
        }
        
        SingleOutputStreamOperator<BlockHeader> blockHeaders;
        if (test_mode) {
            blockHeaders = blockHashes.map(e -> (BlockHeader)null).uid("async_headers_fetcher").name("Headers fetcher").forceNonParallel();
        } else {
            blockHeaders = AsyncDataStream.orderedWait(
                blockHashes,
                new AsyncBlockHeadersFetcher(rpcClientBuilder1),
                60, 
                TimeUnit.SECONDS, 
                BLOCK_FETCHER_CONCURRENCY*env.getParallelism()
            ).uid("async_headers_fetcher").name("Headers fetcher").forceNonParallel();
        }
        
        blockHeaders = blockHeaders.process(new HeaderTimeProcessor()).uid("header_time_processor").name("Header time processor").forceNonParallel();

        blockHeaders = blockHeaders.assignTimestampsAndWatermarks(new WatermarkStrategy<BlockHeader>() {
            @Override
            public WatermarkGenerator<BlockHeader> createWatermarkGenerator(WatermarkGeneratorSupplier.Context arg0) {
                return new WatermarkGenerator<BlockHeader>() {
                    @Override
                    public void onEvent(BlockHeader header, long timestamp, WatermarkOutput wo) {
                        wo.emitWatermark(new Watermark(header.getTime()));
                    }

                    @Override
                    public void onPeriodicEmit(WatermarkOutput arg0) {
                    }
                };
            }
        }.withTimestampAssigner((event, timestamp) -> event.getTime())).uid("timestampamp_assigner").name("Timestamp assigner").forceNonParallel();

        SingleOutputStreamOperator<Block> blocks;
        if (test_mode) {
            blocks = blockHeaders.map(e -> (Block)null).startNewChain().uid("async_block_fetcher").name("Block fetcher");
        } else {
            blocks = AsyncDataStream.orderedWait(
                    blockHeaders.map(e -> e.getHash()).uid("headers_to_hash").name("Block hashes")
                    .forceNonParallel(), 
                    new AsyncBlockFetcher(rpcClientBuilder2), 
                    60, 
                    TimeUnit.SECONDS, 
                    BLOCK_FETCHER_CONCURRENCY
            ).startNewChain().uid("async_block_fetcher").name("Block fetcher");
        }
        
        if (!test_mode && !disable_sinks) {
            AsyncDataStream.unorderedWait(blocks,
                    new BlockSink(sessionBuilder),
                    10, 
                    TimeUnit.MINUTES, 
                    CASSANDRA_CONCURRENT_REQUESTS
            ).uid("async_block_saver").name("Block saver");//.setParallelism(sinkParallelism);
        }
        
        SingleOutputStreamOperator<ConfirmedTransaction> transactions = blocks.flatMap(new FlatMapFunction<Block, ConfirmedTransaction>() {
            @Override
            public void flatMap(Block value, Collector<ConfirmedTransaction> out) throws Exception {
                int txN = 0;
                for (Transaction tx : value.getTx()) {
                    out.collect(new ConfirmedTransaction(tx, value.getHeight(), txN));
                    txN++;
                }
            }
        }).uid("blocks_to_transactions").name("Blocks To Transactions");//.setParallelism(sinkParallelism);

        SingleOutputStreamOperator<Tuple2<TransactionOutput, String>> outputs = transactions.flatMap(new FlatMapFunction<ConfirmedTransaction, Tuple2<TransactionOutput, String>>() {
        @Override
        public void flatMap(ConfirmedTransaction value, Collector<Tuple2<TransactionOutput, String>> out) throws Exception {
                for (TransactionOutput vout : value.getVout()) {
                    out.collect(new Tuple2(vout, value.getTxid()));
                }
            }
        })
        .uid("transactions_to_outputs").name("Outputs");

        KeyedStream<Tuple2<TransactionOutput, String>, String> outputsByOutpoint = outputs.keyBy(e -> e.f1 + e.f0.getN());

        SingleOutputStreamOperator<InputPointer> inputPointers = transactions.flatMap(new FlatMapFunction<ConfirmedTransaction, InputPointer>() {
            @Override
            public void flatMap(ConfirmedTransaction value, Collector<InputPointer> out) throws Exception {
                if (value.getTxN() == 0) return;
                
                for (int i = 0; i < value.getVin().length; i++) {
                    out.collect(new InputPointer(value.getTxid(), value.getVin()[i].getTxid(), value.getVin()[i].getVout(), i));
                }
            }
        }).uid("transactions_to_input_pointers").name("Input Pointers");
        

        SingleOutputStreamOperator<SpentOutput> spentOutputs = inputPointers.keyBy(e -> e.getTxid()+e.getVout())
                .connect(outputsByOutpoint)
                .process(new InputAttacher())
                .uid("input_attacher").name("Input attacher");

        KeyedStream<SpentOutput, String> spentOutputsByTxid = spentOutputs.keyBy(e -> e.getSpending_txid());

        SingleOutputStreamOperator<ConfirmedTransactionWithInputs> fullTxs = spentOutputsByTxid.connect(transactions.keyBy(e -> e.getTxid())).process(new TransactionAttacher())
                .uid("transaction_attacher").name("Transaction attacher");
        
        if (!test_mode && !disable_sinks) {
            AsyncDataStream.unorderedWait(
                fullTxs, 
                new TransactionSink(sessionBuilder), 
                10, 
                TimeUnit.MINUTES, 
                CASSANDRA_CONCURRENT_REQUESTS
            ).uid("async_transaction_sink").name("TransactionSink");//.setParallelism(sinkParallelism);
        }
        
        
        KeyedStream<Tuple2<String, Long>, String> addressTransactionDeltas = fullTxs.flatMap(new FlatMapFunction<ConfirmedTransactionWithInputs, Tuple2<String, Long>>() {
            @Override
            public void flatMap(ConfirmedTransactionWithInputs transaction, Collector<Tuple2<String, Long>> out) throws Exception {
                
                Map<String, Long> addressDeltas = new HashMap<>();
                for (TransactionOutput vout : transaction.getVout()) {
                    if (vout.getScriptPubKey().getAddresses() == null) continue;
                    if (vout.getScriptPubKey().getAddresses().length != 1) continue;
                    String address = vout.getScriptPubKey().getAddresses()[0];
                    long value = Math.round(vout.getValue()*1e8);
                    addressDeltas.compute(address, (key, oldDelta) -> oldDelta == null ? value : oldDelta + value);
                }
                for (TransactionInputWithOutput vin : transaction.getInputsWithOutputs()) {
                    if (vin.getSpentOutput() == null) continue;
                    if (vin.getSpentOutput().getScriptPubKey().getAddresses() == null) continue;
                    if (vin.getSpentOutput().getScriptPubKey().getAddresses().length != 1) continue;
                    String address = vin.getSpentOutput().getScriptPubKey().getAddresses()[0];
                    long value = Math.round(vin.getSpentOutput().getValue()*1e8);
                    addressDeltas.compute(address, (key, oldDelta) -> oldDelta == null ? -value : oldDelta - value);
                }
                for (Map.Entry<String, Long> e : addressDeltas.entrySet()) {
                    out.collect(new Tuple2<String, Long>(e.getKey(), e.getValue()));
                }
            }
        }).uid("address_transaction_deltas").name("Address Transaction Deltas").keyBy(e -> e.f0);
        
       
        String flinkJobName = StringUtils.capitalize(cassandraKeyspace) + " Blockchain Analysis";

        DataStream<Tuple2<Integer, SimpleAddAddressesAndTransactionsOperation[]>> txClusters = fullTxs.process(new ConfirmedTransactionToDisjointSets()).uid("tx_clusters").name("Tx clusters");
        
        DataStream<Tuple2<Integer, SimpleAddAddressesAndTransactionsOperation[]>> blockClusters = txClusters.keyBy(t -> t.f0)
        .process(new BlockClustering())
        .uid("block_clusters").name("Block clusters");
        
        KeyedStream<Tuple2<Integer, SimpleAddAddressesAndTransactionsOperation[]>, Integer> blockClustersByHeight = DataStreamUtils.reinterpretAsKeyedStream(blockClusters, e -> e.f0);
        SingleOutputStreamOperator<RoutableMessage> rms = blockClustersByHeight.process(new BlockClusterProcessor()).uid("block_cluster_processor").name("Block cluster processor");
        
        StatefulFunctionsConfig statefunConfig = StatefulFunctionsConfig.fromEnvironment(env);
        statefunConfig.setFactoryType(MessageFactoryType.WITH_KRYO_PAYLOADS);
        statefunConfig.setFlinkJobName(flinkJobName);
        StatefulFunctionEgressStreams out = StatefulFunctionDataStreamBuilder.builder("address_clustering")
                .withFunctionProvider(UnionFindFunction.TYPE, functionType -> new UnionFindFunction())
                .withDataStreamAsIngress(rms)
                .withConfiguration(statefunConfig)
                .withEgressId(UnionFindFunction.EGRESS)
                .build(env);
        
        SingleOutputStreamOperator<Object> unionFindOps = out.getDataStreamForEgressId(UnionFindFunction.EGRESS).keyBy(e -> e.getAddress()).process(new UnionFindSink(sessionBuilder, checkpointConfig.getCheckpointInterval())).uid("address_clustering_sink").name("Address Clustering Sink");
       
        SingleOutputStreamOperator<Object> addressOps = addressTransactionDeltas.process(new AddressBalanceProcessor()).uid("address_balance_processor").name("Address Balance Processor");
        
        if (!test_mode && !disable_sinks) {
            AsyncDataStream.unorderedWait(addressOps.union(unionFindOps), 
                    new AddressSink(sessionBuilder), 
                    10, 
                    TimeUnit.MINUTES, 
                    CASSANDRA_CONCURRENT_REQUESTS
            ).uid("address_sink").name("Address Sink");
        }
        env.execute(flinkJobName);
    }
    
    public static boolean possiblyCoinJoin(ConfirmedTransactionWithInputs tx) {
        Set<String> inputAddresses = new HashSet<>();
        for (TransactionInputWithOutput vin : tx.getInputsWithOutputs()) {
            try {
                String address = vin.getSpentOutput().getScriptPubKey().getAddresses()[0];
                if (address != null) {
                    inputAddresses.add(address);
                }
            } catch(Exception ex) {
            }
        }
        if (inputAddresses.size() < 2) return false;
        Map<Long, String> outputAmount2Address = new HashMap<>();
        for (TransactionOutput vout : tx.getVout()) {
            String address;
            try {
                address = vout.getScriptPubKey().getAddresses()[0];
                if (address == null) continue;
            } catch(Exception ex) {
                continue;
            }
            long value = Math.round(vout.getValue()*1e8);
            String equalAmountAddress = outputAmount2Address.get(value);
            if (equalAmountAddress != null && !equalAmountAddress.equals(address)) {
                return true;
            } else {
                outputAmount2Address.put(value, address);
            }
        }
        return false;
      }
}
