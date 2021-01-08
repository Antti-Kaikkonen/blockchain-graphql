package io.github.anttikaikkonen.blockchainanalyticsflink;


import io.github.anttikaikkonen.blockchainanalyticsflink.source.StrictlyIncreasingHeaderTimeAssigner;
import io.github.anttikaikkonen.blockchainanalyticsflink.source.AsyncBlockHashFetcher;
import io.github.anttikaikkonen.blockchainanalyticsflink.source.AsyncBlockFetcher;
import io.github.anttikaikkonen.blockchainanalyticsflink.source.AsyncBlockHeadersFetcher;
import io.github.anttikaikkonen.blockchainanalyticsflink.source.BlockHeightSource;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.BlockSink;
import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.CassandraSessionBuilder;
import io.github.anttikaikkonen.blockchainanalyticsflink.models.ConfirmedTransaction;
import io.github.anttikaikkonen.blockchainanalyticsflink.models.ConfirmedTransactionWithInputs;
import io.github.anttikaikkonen.blockchainanalyticsflink.statefun.unionfind.UnionFindFunction;
import io.github.anttikaikkonen.bitcoinrpcclientjava.RpcClient;
import io.github.anttikaikkonen.bitcoinrpcclientjava.models.Block;
import io.github.anttikaikkonen.bitcoinrpcclientjava.models.BlockHeader;
import io.github.anttikaikkonen.bitcoinrpcclientjava.models.TransactionOutput;
import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.AddressSink;
import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.CassandraSessionBuilderImpl;
import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.CreateStatements;
import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.TransactionSink;
import io.github.anttikaikkonen.blockchainanalyticsflink.models.InputPointer;
import io.github.anttikaikkonen.blockchainanalyticsflink.models.SpentOutput;
import io.github.anttikaikkonen.blockchainanalyticsflink.precluster.BlockClusterProcessor;
import io.github.anttikaikkonen.blockchainanalyticsflink.precluster.BlockClustering;
import io.github.anttikaikkonen.blockchainanalyticsflink.precluster.TransactionClustering;
import io.github.anttikaikkonen.blockchainanalyticsflink.precluster.SimpleAddAddressesAndTransactionsOperation;
import io.github.anttikaikkonen.blockchainanalyticsflink.source.HeaderTimestampAssigner;
import io.github.anttikaikkonen.blockchainanalyticsflink.source.RpcClientSupplier;
import io.github.anttikaikkonen.blockchainanalyticsflink.statefun.sink.ClusterWriteAheadLogger;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;
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

public class Main {
    
    
    private static final int BLOCK_FETCHER_CONCURRENCY = 1;
    public static final int CASSANDRA_CONCURRENT_REQUESTS = 100;
    
    public static final String PROPERTIES_CASSANDRA_HOST = "cassandra.host";
    public static final String PROPERTIES_CASSANDRA_KEYSPACE = "cassandra.keyspace";
    public static final String PROPERTIES_BLOCKCHAIN_RPC_URL = "blockchain.rpc.url";
    public static final String PROPERTIES_BLOCKCHAIN_RPC_USERNAME = "blockchain.rpc.username";
    public static final String PROPERTIES_BLOCKCHAIN_RPC_PASSWORD = "blockchain.rpc.password";
    public static final String PROPERTIES_CONCURRENT_BLOCKS = "concurrent_blocks";
    public static final String PROPERTIES_CHECKPOINT_INTERVAL = "checkpoint_interval";
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
        final int checkpointInterval;
        if (test_mode) {
            cassandraHosts = null;
            cassandraKeyspace = null;
            blockchainRpcURLS = null;
            blockchainUsername = null;
            blockchainPassword = null;
            concurrentBlocks = -1;
            disable_sinks=true;
            checkpointInterval = 60000*10;
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
            
            checkpointInterval = properties.getInt(PROPERTIES_CHECKPOINT_INTERVAL, 60000*10);
        }
        System.out.println("KEYSPACE = "+cassandraKeyspace);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(checkpointInterval, CheckpointingMode.EXACTLY_ONCE);
        env.setRestartStrategy(RestartStrategies.failureRateRestart(5, Time.of(3, TimeUnit.MINUTES), Time.of(10, TimeUnit.SECONDS)));//Allow 5 restarts within 3 minutes
        env.getConfig().disableAutoGeneratedUIDs();
        env.getConfig().enableObjectReuse();
        
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setMinPauseBetweenCheckpoints(checkpointInterval);
        checkpointConfig.setCheckpointTimeout(Long.MAX_VALUE);//Checkpoints can take hours if they are triggered when flink is restoring a large state
        checkpointConfig.enableUnalignedCheckpoints(false);//Unaligned checkpoints do not work with stateful functions
        checkpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        
        CassandraSessionBuilder sessionBuilder = new CassandraSessionBuilderImpl(cassandraHosts, cassandraKeyspace);
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
            session.execute(CreateStatements.TABLE_CLUSTER_DETAILS);
            session.execute(CreateStatements.TABLE_CLUSTER_DAILY_BALANCE_CHANGE);
            session.execute(CreateStatements.VIEW_CLUSTER_TOP_GAINERS);
            session.execute(CreateStatements.VIEW_CLUSTER_TOP_LOSERS);
            session.execute(CreateStatements.VIEW_CLUSTER_RICHLIST);
            session.close();
            session.getCluster().close();
        }
        //End of schema creation
        int parallelism = env.getParallelism();
        
        Supplier<RpcClient> rpcClientSupplier1;
        Supplier<RpcClient> rpcClientSupplier2;
        if (test_mode) {
            rpcClientSupplier1 = null;
            rpcClientSupplier2 = null;
        } else {
            rpcClientSupplier1 = new RpcClientSupplier(blockchainUsername, blockchainPassword, blockchainRpcURLS, parallelism, parallelism * blockchainRpcURLS.length);
            rpcClientSupplier2 = new RpcClientSupplier(blockchainUsername, blockchainPassword, blockchainRpcURLS, 1, 1 * blockchainRpcURLS.length);
        }

        SingleOutputStreamOperator<Integer> blockHeights;
        if (test_mode) {
            blockHeights = env.addSource(new SourceFunction<Integer>() {
                @Override
                public void run(SourceContext<Integer> ctx) throws Exception {
                }

                @Override
                public void cancel() {
                }
            }).uid("Block height source").name("Block height source");
        } else {
            BlockHeightSource blockHeightSource = BlockHeightSource.builder()
                    .minConfirmations(5)
                    .rpcClientSupplier(rpcClientSupplier1)
                    .sessionBuilder(disable_sinks ? null : sessionBuilder)
                    .concurrentBlocks(concurrentBlocks)
                    .build();
            blockHeights = env.addSource(blockHeightSource).uid("Block height source").name("Block height source");
        }
        
        SingleOutputStreamOperator<String> blockHashes;
        if (test_mode) {
            blockHashes = blockHeights.map(e -> (String)null).uid("Block hash fetcher").name("Block hash fetcher").forceNonParallel();
        } else {
            blockHashes = AsyncDataStream.orderedWait(
                blockHeights,
                new AsyncBlockHashFetcher(rpcClientSupplier1),
                60, 
                TimeUnit.SECONDS, 
                BLOCK_FETCHER_CONCURRENCY*env.getParallelism()
            ).uid("Block hash fetcher").name("Block hash fetcher").forceNonParallel();
        }
        
        SingleOutputStreamOperator<BlockHeader> blockHeaders;
        if (test_mode) {
            blockHeaders = blockHashes.map(e -> (BlockHeader)null).uid("Block headers fetcher").name("Block headers fetcher").forceNonParallel();
        } else {
            blockHeaders = AsyncDataStream.orderedWait(
                blockHashes,
                new AsyncBlockHeadersFetcher(rpcClientSupplier1),
                60, 
                TimeUnit.SECONDS, 
                BLOCK_FETCHER_CONCURRENCY*env.getParallelism()
            ).uid("Block headers fetcher").name("Block headers fetcher").forceNonParallel();
        }
        
        blockHeaders = blockHeaders.process(new StrictlyIncreasingHeaderTimeAssigner())
                .uid("Strictly increasing header time assigner").name("Strictly increasing header time assigner").forceNonParallel();

        blockHeaders = blockHeaders.assignTimestampsAndWatermarks(new HeaderTimestampAssigner())
                .uid("Header timestamp assigner").name("Header timestamp assigner").forceNonParallel();

        SingleOutputStreamOperator<Block> blocks;
        if (test_mode) {
            blocks = blockHeaders.map(e -> (Block)null).startNewChain().uid("Block fetcher").name("Block fetcher");
        } else {
            blocks = AsyncDataStream.orderedWait(
                    blockHeaders.map(e -> e.getHash()).uid("Header to block hash").name("Header to block hash")
                    .forceNonParallel(), 
                    new AsyncBlockFetcher(rpcClientSupplier2), 
                    60, 
                    TimeUnit.SECONDS, 
                    BLOCK_FETCHER_CONCURRENCY
            ).startNewChain().uid("Block fetcher").name("Block fetcher");
        }
        
        if (!test_mode && !disable_sinks) {
            AsyncDataStream.unorderedWait(blocks,
                    new BlockSink(sessionBuilder),
                    10, 
                    TimeUnit.MINUTES, 
                    CASSANDRA_CONCURRENT_REQUESTS
            ).uid("Block sink").name("Block sink");
        }
        
        SingleOutputStreamOperator<ConfirmedTransaction> transactions = blocks.flatMap(new BlocksToTransactions())
                .uid("Blocks to transactions").name("Blocks to transactions");

        SingleOutputStreamOperator<Tuple2<TransactionOutput, String>> outputs = transactions.flatMap(new TransactionsToOutputs())
                .uid("Transactions to outputs").name("Transactions to outputs");

        KeyedStream<Tuple2<TransactionOutput, String>, String> outputsByOutpoint = outputs.keyBy(e -> e.f1 + e.f0.getN());

        SingleOutputStreamOperator<InputPointer> inputPointers = transactions.flatMap(new TransactionsToInputPointers())
                .uid("Transactions to input pointers").name("Transactions to input pointers");
        

        SingleOutputStreamOperator<SpentOutput> spentOutputs = inputPointers.keyBy(e -> e.getTxid()+e.getVout())
                .connect(outputsByOutpoint)
                .process(new InputAttacher())
                .uid("Input attacher").name("Input attacher");

        KeyedStream<SpentOutput, String> spentOutputsByTxid = spentOutputs.keyBy(e -> e.getSpending_txid());

        SingleOutputStreamOperator<ConfirmedTransactionWithInputs> fullTxs = spentOutputsByTxid.connect(transactions.keyBy(e -> e.getTxid())).process(new TransactionAttacher())
                .uid("Transaction attacher").name("Transaction attacher");
        
        if (!test_mode && !disable_sinks) {
            AsyncDataStream.unorderedWait(
                fullTxs, 
                new TransactionSink(sessionBuilder), 
                10, 
                TimeUnit.MINUTES, 
                CASSANDRA_CONCURRENT_REQUESTS
            ).uid("Transaction sink").name("Transaction sink");
        }
        
        
        KeyedStream<Tuple2<String, Long>, String> addressTransactionDeltas = fullTxs.flatMap(new TransactionsToAddressBalanceChanges())
                .uid("Transactions to address balance changes").name("Transactions to address balance changes").keyBy(e -> e.f0);
        
       
        String flinkJobName = StringUtils.capitalize(cassandraKeyspace) + " Blockchain Analysis";

        DataStream<Tuple2<Integer, SimpleAddAddressesAndTransactionsOperation[]>> txClusters = fullTxs.process(new TransactionClustering())
                .uid("Transaction clustering").name("Transaction clustering");
        
        DataStream<Tuple2<Integer, SimpleAddAddressesAndTransactionsOperation[]>> blockClusters = txClusters.keyBy(t -> t.f0)
                .process(new BlockClustering())
                .uid("Block clustering").name("Block clustering");
        
        KeyedStream<Tuple2<Integer, SimpleAddAddressesAndTransactionsOperation[]>, Integer> blockClustersByHeight = DataStreamUtils.reinterpretAsKeyedStream(blockClusters, e -> e.f0);
        
        SingleOutputStreamOperator<RoutableMessage> rms = blockClustersByHeight.process(new BlockClusterProcessor())
                .uid("Block cluster processor").name("Block cluster processor");
        
        StatefulFunctionsConfig statefunConfig = StatefulFunctionsConfig.fromEnvironment(env);
        statefunConfig.setFactoryType(MessageFactoryType.WITH_KRYO_PAYLOADS);
        statefunConfig.setFlinkJobName(flinkJobName);
        StatefulFunctionEgressStreams out = StatefulFunctionDataStreamBuilder.builder("address_clustering")
                .withFunctionProvider(UnionFindFunction.TYPE, functionType -> new UnionFindFunction())
                .withDataStreamAsIngress(rms)
                .withConfiguration(statefunConfig)
                .withEgressId(UnionFindFunction.EGRESS)
                .build(env);
        
        SingleOutputStreamOperator<Object> unionFindOps = out.getDataStreamForEgressId(UnionFindFunction.EGRESS)
                .keyBy(e -> e.getAddress()).process(new ClusterWriteAheadLogger(sessionBuilder, checkpointInterval))
                .uid("Cluster write-ahead logger").name("Cluster write-ahead logger");
       
        SingleOutputStreamOperator<Object> addressOps = addressTransactionDeltas.process(new AddressBalanceProcessor())
                .uid("Address balance processor").name("Address balance processor");
        
        if (!test_mode && !disable_sinks) {
            AsyncDataStream.unorderedWait(addressOps.union(unionFindOps), 
                    new AddressSink(sessionBuilder), 
                    10, 
                    TimeUnit.MINUTES, 
                    CASSANDRA_CONCURRENT_REQUESTS
            ).uid("Address sink").name("Address sink");
        }
        env.execute(flinkJobName);
    }
    
 
}
