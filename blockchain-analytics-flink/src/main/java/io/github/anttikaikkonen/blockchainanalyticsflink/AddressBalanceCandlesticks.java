package io.github.anttikaikkonen.blockchainanalyticsflink;


import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.AddressBalanceSaver;
import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.AddressTransactionSaver;
import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.BlockSaver;
import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.CassandraSessionBuilder;
import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.ConfirmedTransactionSaver;
import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.GainersAndLosersSaver;
import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.InputSaver;
import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.OHLCSaver;
import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.OutputSaver;
import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.RichListSaver;
import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.AddressTransaction;
import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.OHLC;
import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.RichList;
import io.github.anttikaikkonen.blockchainanalyticsflink.models.AddressBalanceChange;
import io.github.anttikaikkonen.blockchainanalyticsflink.models.AddressBalanceUpdate;
import io.github.anttikaikkonen.blockchainanalyticsflink.models.ConfirmedTransaction;
import io.github.anttikaikkonen.blockchainanalyticsflink.models.ConfirmedTransactionWithInputs;
import io.github.anttikaikkonen.blockchainanalyticsflink.models.TransactionInputWithOutput;
import io.github.anttikaikkonen.blockchainanalyticsflink.statefun.cassandraexecutor.AddressOperation;
import io.github.anttikaikkonen.blockchainanalyticsflink.statefun.sink.UnionFindSink;
import io.github.anttikaikkonen.blockchainanalyticsflink.statefun.unionfind.AddTransactionOperation;
import io.github.anttikaikkonen.blockchainanalyticsflink.statefun.unionfind.MergeOperation;
import io.github.anttikaikkonen.blockchainanalyticsflink.statefun.unionfind.UnionFindFunction;
import io.github.anttikaikkonen.bitcoinrpcclientjava.LeastConnectionsRpcClient;
import io.github.anttikaikkonen.bitcoinrpcclientjava.LimitedCapacityRpcClient;
import io.github.anttikaikkonen.bitcoinrpcclientjava.RpcClient;
import io.github.anttikaikkonen.bitcoinrpcclientjava.models.Block;
import io.github.anttikaikkonen.bitcoinrpcclientjava.models.BlockHeader;
import io.github.anttikaikkonen.bitcoinrpcclientjava.models.Transaction;
import io.github.anttikaikkonen.bitcoinrpcclientjava.models.TransactionInput;
import io.github.anttikaikkonen.bitcoinrpcclientjava.models.TransactionOutput;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.statefun.flink.core.StatefulFunctionsConfig;
import org.apache.flink.statefun.flink.core.message.MessageFactoryType;
import org.apache.flink.statefun.flink.core.message.RoutableMessage;
import org.apache.flink.statefun.flink.core.message.RoutableMessageBuilder;
import org.apache.flink.statefun.flink.datastream.StatefulFunctionDataStreamBuilder;
import org.apache.flink.statefun.flink.datastream.StatefulFunctionEgressStreams;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.Collector;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.reactor.IOReactorConfig;

public class AddressBalanceCandlesticks {
    
    private static final int CASSANDRA_CONCURRENT_REQUESTS = 3;
    private static final int CASSANDRA_TIMEOUT = 60000;
    
    
    public static final String PROPERTIES_CASSANDRA_HOST = "cassandra.host";
    public static final String PROPERTIES_CASSANDRA_KEYSPACE = "cassandra.namespace";
    public static final String PROPERTIES_BLOCKCHAIN_RPC_URL = "blockchain.rpc.url";
    public static final String PROPERTIES_BLOCKCHAIN_RPC_USERNAME = "blockchain.rpc.username";
    public static final String PROPERTIES_BLOCKCHAIN_RPC_PASSWORD = "blockchain.rpc.password";
    
    public static void main(String[] args) throws Exception {
        
        ParameterTool properties = ParameterTool.fromArgs(args);
        String propertiesFile = properties.get("properties-file");
        if (propertiesFile != null) {
            ParameterTool fileProperties = ParameterTool.fromPropertiesFile(propertiesFile);
            properties = fileProperties.mergeWith(properties);//arguments overwrite file properties
        }
        
        String cassandraHost = properties.getRequired(PROPERTIES_CASSANDRA_HOST);
        String[] cassandraHosts = cassandraHost.split("\\s+");
        
        
        String cassandraKeyspace = properties.getRequired(PROPERTIES_CASSANDRA_KEYSPACE);
        String blockchainRpcURL = properties.getRequired(PROPERTIES_BLOCKCHAIN_RPC_URL);
        String[] blockchainRpcURLS = blockchainRpcURL.split("\\s+");
        System.out.println("RPC URLCS = "+Arrays.toString(blockchainRpcURLS));
        String blockchainUsername = properties.getRequired(PROPERTIES_BLOCKCHAIN_RPC_USERNAME);
        String blockchainPassword = properties.getRequired(PROPERTIES_BLOCKCHAIN_RPC_PASSWORD);
        
        System.out.println("ARGS: "+Arrays.toString(args));
        
        CassandraSessionBuilder sessionBuilder = new CassandraSessionBuilder() {
            @Override
            protected Session createSession(Cluster.Builder builder) {
                Cluster cluster = builder
                        .addContactPoints(cassandraHosts)
                        .withSocketOptions(
                                new SocketOptions()
                                        .setReadTimeoutMillis(CASSANDRA_TIMEOUT)
                                        .setKeepAlive(true)
                        )
                        .build();
                Session session = cluster.connect(cassandraKeyspace);
                
                return session;
            }
       };
        RpcClientBuilder rpcClientBuilder = new RpcClientBuilder() {
            @Override
            public RpcClient build() {
                
                CredentialsProvider provider = new BasicCredentialsProvider();
                provider.setCredentials(
                        AuthScope.ANY,
                        new UsernamePasswordCredentials(blockchainUsername, blockchainPassword)
                );


                IOReactorConfig ioReactorConfig = IOReactorConfig.custom()
                        .setIoThreadCount(Math.min(Runtime.getRuntime().availableProcessors(), 2))
                        .build();

                CloseableHttpAsyncClient httpClient = HttpAsyncClients.custom()
                        .setDefaultIOReactorConfig(ioReactorConfig)
                        .setDefaultCredentialsProvider(provider)
                        .setMaxConnPerRoute(4)
                        .setMaxConnTotal(4)
                        .build();
                httpClient.start();
                
                RpcClient[] clients =  new RpcClient[blockchainRpcURLS.length];
                for (int i = 0; i < clients.length; i++) {
                    clients[i] = new LimitedCapacityRpcClient(httpClient, blockchainRpcURLS[i].trim(), 2);
                }
                return new LeastConnectionsRpcClient(clients);
            }
        };


        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        System.out.println("env.getParallelism()):"+env.getParallelism());
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(60000*15, CheckpointingMode.EXACTLY_ONCE);
        env.setRestartStrategy(RestartStrategies.failureRateRestart(5, Time.of(3, TimeUnit.MINUTES), Time.of(10, TimeUnit.SECONDS)));//Allow 5 restarts within 3 minutes
        env.getConfig().disableAutoGeneratedUIDs();
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setMinPauseBetweenCheckpoints(60000*15);
        checkpointConfig.setCheckpointTimeout(60000*60);//60 minutes
        checkpointConfig.enableUnalignedCheckpoints(false);
        //checkpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        HeadersSource bhs = HeadersSource.builder()
                .minConfirmations(5)
                .rpcClientBuilder(rpcClientBuilder)
                .sessionBuilder(sessionBuilder)
                .build();
        
        SingleOutputStreamOperator<BlockHeader> blockHeaders = env.addSource(bhs).uid("headers_source").name("Block headers source");
        SingleOutputStreamOperator<Block> blocks = AsyncDataStream.orderedWait(
                blockHeaders.map(e -> e.getHash()).uid("headers_to_hash").name("Block hashes")
                .setParallelism(1), 
                new AsyncBlockFetcher(rpcClientBuilder), 
                10000, 
                TimeUnit.MILLISECONDS, 
                8
        ).uid("async_block_fetcher").name("Block fetcher");
        
        
        
        AsyncDataStream.orderedWait(
                blocks,
                new BlockSaver(sessionBuilder), 
                CASSANDRA_TIMEOUT, 
                TimeUnit.MILLISECONDS, 
                CASSANDRA_CONCURRENT_REQUESTS
        ).uid("async_block_saver").name("Block saver");
        

        SingleOutputStreamOperator<ConfirmedTransaction> transactions = blocks.flatMap(new FlatMapFunction<Block, ConfirmedTransaction>() {
            @Override
            public void flatMap(Block value, Collector<ConfirmedTransaction> out) throws Exception {
                int txN = 0;
                for (Transaction tx : value.getTx()) {
                    out.collect(new ConfirmedTransaction(tx, value.getHeight(), txN));
                    txN++;
                }
            }
        }).uid("blocks_to_transactions").name("Blocks To Transactions");
        
        SingleOutputStreamOperator<Tuple2<TransactionOutput, String>> outputs = transactions.flatMap(new FlatMapFunction<ConfirmedTransaction, Tuple2<TransactionOutput, String>>() {
        @Override
        public void flatMap(ConfirmedTransaction value, Collector<Tuple2<TransactionOutput, String>> out) throws Exception {
                for (TransactionOutput vout : value.getVout()) {
                    out.collect(new Tuple2(vout, value.getTxid()));
                }
            }
        })
        .uid("transactions_to_outputs").name("Outputs");//.setParallelism(1);

        
        AsyncDataStream.orderedWait(
                outputs, 
                new OutputSaver(sessionBuilder), 
                CASSANDRA_TIMEOUT, 
                TimeUnit.MILLISECONDS, 
                CASSANDRA_CONCURRENT_REQUESTS
        ).uid("async_output_saver").name("Output saver");
        

        KeyedStream<Tuple2<TransactionOutput, String>, String> outputsByOutpoint = outputs.keyBy(e -> e.f1 + e.f0.getN());

        SingleOutputStreamOperator<Tuple3<TransactionInput, String, Integer>> inputs = transactions.flatMap(new FlatMapFunction<ConfirmedTransaction, Tuple3<TransactionInput, String, Integer>>() {
            @Override
            public void flatMap(ConfirmedTransaction value, Collector<Tuple3<TransactionInput, String, Integer>> out) throws Exception {
                int index = 0;
                for (TransactionInput vin : value.getVin()) {
                    out.collect(new Tuple3(vin, value.getTxid(), index));
                    index++;
                }
            }
        }).uid("transactions_to_inputs").name("Inputs");//.setParallelism(1)
        
        
        AsyncDataStream.orderedWait(
                inputs, 
                new InputSaver(sessionBuilder), 
                CASSANDRA_TIMEOUT, 
                TimeUnit.MILLISECONDS, 
                CASSANDRA_CONCURRENT_REQUESTS
        ).uid("async_input_saver").name("Input saver");
        
        
        SingleOutputStreamOperator<Tuple2<TransactionInput, String>> nonCoinbaseInputs = inputs
                .filter(input -> input.f0.getTxid() != null).uid("non_coinbase_filter").name("Non coinbase inputs")
                .map(new MapFunction<Tuple3<TransactionInput, String, Integer>, Tuple2<TransactionInput, String>>() {
                    @Override
                    public Tuple2<TransactionInput, String> map(Tuple3<TransactionInput, String, Integer> e) throws Exception {
                        return new Tuple2<TransactionInput, String>(e.f0, e.f1);
                    }
                })
                .uid("non_coinbase_inputs").name("Non coinbase input tuples"); //.setParallelism(1);

        KeyedStream<Tuple2<TransactionInput, String>, String> inputsByOutpoint = nonCoinbaseInputs.keyBy(e -> e.f0.getTxid() + e.f0.getVout());

        
        SingleOutputStreamOperator<Tuple2<TransactionInputWithOutput, String>> spentOutputs = inputsByOutpoint
                .connect(outputsByOutpoint)
                .process(new InputAttacher())
                .uid("input_attacher").name("Input attacher");


        KeyedStream<Tuple2<TransactionInputWithOutput, String>, String> spentOutputsByOutpoint = DataStreamUtils.reinterpretAsKeyedStream(spentOutputs, e -> e.f0.getTxid()+e.f0.getVout());

        DataStreamUtils.reinterpretAsKeyedStream(spentOutputs, e -> e.f1);
        KeyedStream<Tuple2<TransactionInputWithOutput, String>, String> spentOutputsByTxid = spentOutputsByOutpoint.keyBy(e -> e.f1);

        SingleOutputStreamOperator<ConfirmedTransactionWithInputs> fullTxs = spentOutputsByTxid.connect(transactions.keyBy(e -> e.getTxid())).process(new TransactionAttacher())
                .uid("transaction_attacher").name("Transaction attacher");
                
        SingleOutputStreamOperator<io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.Transaction> txs = fullTxs.map(transaction -> {
            io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.Transaction tx = new io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.Transaction();
            tx.setHeight(transaction.getHeight());
            tx.setLocktime(transaction.getLocktime());
            tx.setSize(transaction.getSize());
            tx.setTxN(transaction.getTxN());
            tx.setTxid(transaction.getTxid());
            tx.setVersion(transaction.getVersion());
            tx.setOutput_count(transaction.getVout().length);
            tx.setInput_count(transaction.getVin().length);
            long fee = 0;
            if (transaction.getTxN() > 0) {
                for (TransactionInputWithOutput vin : transaction.getVin()) {
                    fee += Math.round(vin.getSpentOutput().getValue()*1e8);
                }
                for (TransactionOutput vout : transaction.getVout()) {
                    fee -= Math.round(vout.getValue()*1e8);
                }
            }
            tx.setTx_fee(fee);
            return tx;
        }).uid("full_transaction_to_cassandra_transaction").name("Transaction fee calculator");
        
        
        AsyncDataStream.orderedWait(txs, new ConfirmedTransactionSaver(sessionBuilder), 
                CASSANDRA_TIMEOUT, 
                TimeUnit.MILLISECONDS, 
                CASSANDRA_CONCURRENT_REQUESTS
        ).uid("async_confirmed_transaction_saver").name("Confirmed transaction saver");
        
        

        SingleOutputStreamOperator<AddressTransaction> addressTransactions = fullTxs.flatMap(new FlatMapFunction<ConfirmedTransactionWithInputs, AddressTransaction>() {
            @Override
            public void flatMap(ConfirmedTransactionWithInputs tx, Collector<AddressTransaction> out) throws Exception {
                Map<String, Long> addressDeltas = new HashMap<>();
                for (TransactionOutput vout : tx.getVout()) {
                    if (vout.getScriptPubKey().getAddresses() == null) continue;
                    if (vout.getScriptPubKey().getAddresses().length != 1) continue;
                    String address = vout.getScriptPubKey().getAddresses()[0];
                    long value = Math.round(vout.getValue()*1e8);
                    addressDeltas.compute(address, (key, oldDelta) -> oldDelta == null ? value : oldDelta + value);
                }
                for (TransactionInputWithOutput vin : tx.getVin()) {
                    if (vin.getSpentOutput() == null) continue;
                    if (vin.getSpentOutput().getScriptPubKey().getAddresses() == null) continue;
                    if (vin.getSpentOutput().getScriptPubKey().getAddresses().length != 1) continue;
                    String address = vin.getSpentOutput().getScriptPubKey().getAddresses()[0];
                    long value = Math.round(vin.getSpentOutput().getValue()*1e8);
                    addressDeltas.compute(address, (key, oldDelta) -> oldDelta == null ? -value : oldDelta - value);
                }
                for (String address : addressDeltas.keySet()) {
                    long delta = addressDeltas.get(address);
                    AddressTransaction res = new AddressTransaction(address, null, tx.getHeight(), tx.getTxN(), delta);
                    out.collect(res);
                }
            }
        }).uid("transaction_to_address_transactions").name("Address transactions")
        .process(new ProcessFunction<AddressTransaction, AddressTransaction>() {
            @Override
            public void processElement(AddressTransaction value, Context ctx, Collector<AddressTransaction> out) throws Exception {
                
                value.setTimestamp( Date.from(Instant.ofEpochMilli(ctx.timestamp())));
                out.collect(value);
            }
        }).uid("timestamp_to_address_transaction").name("Address Transactions With Timestamp");

        
        AsyncDataStream.orderedWait(addressTransactions, new AddressTransactionSaver(sessionBuilder), CASSANDRA_TIMEOUT, TimeUnit.MILLISECONDS, CASSANDRA_CONCURRENT_REQUESTS)
                .uid("async_address_transactions_saver").name("AddressTransaction saver");
       

        SingleOutputStreamOperator<Tuple2<String, Long>> plusDeltas = outputs
                .filter(e -> e.f0.getScriptPubKey().getAddresses() != null && e.f0.getScriptPubKey().getAddresses().length == 1)//.setParallelism(1)
                .uid("single_address_output_filter").name("Outputs with a single address")
                .map(e -> new Tuple2<String, Long>(e.f0.getScriptPubKey().getAddresses()[0], Math.round(e.f0.getValue()*1e8)), TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {}))//.setParallelism(1)
                .uid("output_to_minusdelta").name("Positive AddressDelta");

        SingleOutputStreamOperator<Tuple2<String, Long>> minusDeltas = spentOutputsByOutpoint
                .filter(e -> e.f0.getSpentOutput().getScriptPubKey().getAddresses() != null && e.f0.getSpentOutput().getScriptPubKey().getAddresses().length == 1).uid("single_address_spent_output_filter").name("Spent outputs with a single address")
                .map(e -> new Tuple2<String, Long>(e.f0.getSpentOutput().getScriptPubKey().getAddresses()[0], Math.round(-e.f0.getSpentOutput().getValue()*1e8)), TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {}))
                .uid("spent_output_to_minusdelta").name("Negative AddressDelta");

        SingleOutputStreamOperator<Tuple2<String, Long>> addressBlockDeltas = plusDeltas
                .union(minusDeltas)
                .keyBy(e -> e.f0)
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.milliseconds(1)))
                .reduce((a, b) -> {
                    return new Tuple2<>(a.f0, a.f1+b.f1);
                }).uid("block_address_deltas_aggegator").name("Address block delta");

        KeyedStream<Tuple2<String, Long>, String> addressBlockDeltasByAddress = DataStreamUtils.reinterpretAsKeyedStream(addressBlockDeltas, e -> e.f0);

        SingleOutputStreamOperator<Tuple2<String, Long>> dailyDeltas = addressBlockDeltasByAddress
        .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.days(1)))
        .reduce((a, b) -> {
            return new Tuple2<>(a.f0, a.f1+b.f1);
        }).uid("block_deltas_to_daily_deltas_aggregator").name("Daily deltas")
        .filter(e -> e.f1 != 0).uid("non_zero_daily_delta_filter").name("Non zero daily delta");

        KeyedStream<Tuple2<String, Long>, String> dailyDeltasByAddress = DataStreamUtils.reinterpretAsKeyedStream(dailyDeltas, e -> e.f0);

        SingleOutputStreamOperator<AddressBalanceChange> dailyGainersAndLosers = dailyDeltasByAddress//dailyDeltas
        .process(new KeyedProcessFunction<String, Tuple2<String, Long>, AddressBalanceChange>() {
            @Override
            public void processElement(Tuple2<String, Long> value, Context ctx, Collector<AddressBalanceChange> out) throws Exception {
                AddressBalanceChange res = new AddressBalanceChange(LocalDate.fromDaysSinceEpoch((int) (ctx.timestamp()/(1000*60*60*24))), value.f0, value.f1);
                out.collect(res);
            }
        }).uid("address_deltas_to_address_balance_changess").name("Address balance change");

        KeyedStream<AddressBalanceChange, String> dailyGainersAndLosersByAddress = DataStreamUtils.reinterpretAsKeyedStream(dailyGainersAndLosers, e -> e.getAddress());

        
        AsyncDataStream.orderedWait(
                dailyGainersAndLosersByAddress, 
                new GainersAndLosersSaver(sessionBuilder), 
                CASSANDRA_TIMEOUT, 
                TimeUnit.MILLISECONDS, 
                CASSANDRA_CONCURRENT_REQUESTS
        ).uid("async_gainers_and_losers_saver").name("GainersAndLosers saver");
        


        SingleOutputStreamOperator<RichList> dailyBigBalances = dailyDeltasByAddress
        .process(new AddressBalanceDailyUpdater(Math.round(1e8*100))).uid("daily_deltas_to_big_balances").name("Richlist");


        KeyedStream<RichList, String> dailyBigBalancesByAddress = DataStreamUtils.reinterpretAsKeyedStream(dailyBigBalances, e-> e.getAddress());

       
        AsyncDataStream.orderedWait(
                dailyBigBalancesByAddress, 
                new RichListSaver(sessionBuilder), 
                CASSANDRA_TIMEOUT, 
                TimeUnit.MILLISECONDS, 
                CASSANDRA_CONCURRENT_REQUESTS
        ).uid("async_richlist_saver").name("Richlist saver");
       

        SingleOutputStreamOperator<AddressBalanceUpdate> addressBalances = addressBlockDeltasByAddress
                .process(new AddressBalanceUpdater())
                .uid("address_balance_updater").name("AddressBalanceUpdate");

        KeyedStream<AddressBalanceUpdate, String> addressBalancesByAddress = DataStreamUtils.reinterpretAsKeyedStream(addressBalances, e -> e.getAddress());

       
        AsyncDataStream.orderedWait(addressBalancesByAddress, 
                new AddressBalanceSaver(sessionBuilder), 
                CASSANDRA_TIMEOUT, 
                TimeUnit.MILLISECONDS, 
                CASSANDRA_CONCURRENT_REQUESTS
        ).uid("async_address_balances_saver").name("AddressBalances saver");
       

        //Candlesticks
        SingleOutputStreamOperator<OHLC> dailyCandlesticks = addressBalancesByAddress
            .timeWindow(org.apache.flink.streaming.api.windowing.time.Time.days(1))
            .aggregate(new AggregateFunction<AddressBalanceUpdate, OHLC, OHLC>() {
                        @Override
                        public OHLC createAccumulator() {
                            return new OHLC();
                        }

                        @Override
                        public OHLC add(AddressBalanceUpdate value, OHLC accumulator) {
                            if (accumulator.getOpen() == null) {
                                accumulator.setAddress(value.getAddress());
                                //long previousBalance = value.getBalance()-value.getBalanceChange();
                                accumulator.setOpen(value.getPreviousBalance());
                                accumulator.setHigh(Math.max(value.getBalance(), value.getPreviousBalance()));
                                accumulator.setLow(Math.min(value.getBalance(), value.getPreviousBalance()));
                            } else {
                                if (!accumulator.getAddress().equals(value.getAddress())) {
                                    System.out.println("ADDRESS CHANGED");
                                }
                                if (value.getBalance() > accumulator.getHigh()) {
                                    accumulator.setHigh(value.getBalance());
                                } else if (value.getBalance() < accumulator.getLow()) {
                                    accumulator.setLow(value.getBalance());
                                }
                            }
                            accumulator.setClose(value.getBalance());
                            return accumulator;
                        }

                        @Override
                        public OHLC getResult(OHLC accumulator) {
                            return accumulator;
                        }

                        @Override
                        public OHLC merge(OHLC a, OHLC b) {
                            System.out.println("MERGE");
                            return a;
                        }
                    },
                    new ProcessOHLCWindow()
            ).uid("ohlc_aggregator").name("OHLC");

        
        AsyncDataStream.orderedWait(
                dailyCandlesticks, 
                new OHLCSaver(sessionBuilder), 
                CASSANDRA_TIMEOUT, 
                TimeUnit.MILLISECONDS, 
                CASSANDRA_CONCURRENT_REQUESTS
        ).uid("async_ohlc_saver").name("OHLC saver");
       

        SingleOutputStreamOperator<RoutableMessage> rms = fullTxs.process(new ProcessFunction<ConfirmedTransactionWithInputs, RoutableMessage>() {
            @Override
            public void processElement(ConfirmedTransactionWithInputs cti, Context ctx, Collector<RoutableMessage> out) throws Exception {
                long timestamp = ctx.timestamp();
                if (!possiblyCoinJoin(cti)) {
                    Set<String> inputAddresses = new TreeSet<>();
                    long inputDelta = 0;
                    for (TransactionInputWithOutput vin : cti.getVin()) {
                        try {
                            String address = vin.getSpentOutput().getScriptPubKey().getAddresses()[0];
                            if (address != null) {
                                inputAddresses.add(address);
                                inputDelta -= Math.round(vin.getSpentOutput().getValue()*1e8);
                            }
                        } catch(Exception ex) {
                        }
                    }
                    String firstAddress = null;
                    AddTransactionOperation addTransactionOperation = new AddTransactionOperation(timestamp, cti.getHeight(), cti.getTxN(), inputDelta);
                    for (String address : inputAddresses) {
                        if (firstAddress == null) {
                            firstAddress = address;
                        } else {
                            MergeOperation mergeOp = new MergeOperation(firstAddress, new ArrayList<>(), addTransactionOperation);
                            RoutableMessage rm = RoutableMessageBuilder.builder().withTargetAddress(new Address(UnionFindFunction.TYPE, address)).withMessageBody(mergeOp).build();
                            out.collect(rm);
                            addTransactionOperation = null;
                        }
                    }
                    if (inputAddresses.size() == 1) {
                        RoutableMessage rm = RoutableMessageBuilder.builder().withTargetAddress(new Address(UnionFindFunction.TYPE, firstAddress)).withMessageBody(addTransactionOperation).build();
                        out.collect(rm);
                    }
                } else {
                    for (TransactionInputWithOutput vin : cti.getVin()) {
                        try {
                            String address = vin.getSpentOutput().getScriptPubKey().getAddresses()[0];
                            if (address != null) {
                                AddTransactionOperation txOp = new AddTransactionOperation(timestamp, cti.getHeight(), cti.getTxN(), -Math.round(vin.getSpentOutput().getValue()*1e8));
                                RoutableMessage rm = RoutableMessageBuilder.builder().withTargetAddress(new Address(UnionFindFunction.TYPE, address)).withMessageBody(txOp).build();
                                out.collect(rm);
                            }
                        } catch(Exception ex) {
                        }
                        
                    }
                }
                for (TransactionOutput vout : cti.getVout()) {
                    try {
                        String address = vout.getScriptPubKey().getAddresses()[0];
                        if (address != null) {
                           AddTransactionOperation txOp = new AddTransactionOperation(timestamp, cti.getHeight(), cti.getTxN(), Math.round(vout.getValue()*1e8));
                           RoutableMessage rm = RoutableMessageBuilder.builder().withTargetAddress(new Address(UnionFindFunction.TYPE, address)).withMessageBody(txOp).build();
                           out.collect(rm);
                        }
                    } catch(Exception ex) {  
                    }
                }
            }
            
        }).name("Routable messages").uid("routable_messages");
        
        
        String flinkJobName = StringUtils.capitalize(cassandraKeyspace) + " Address Analysis";
        
        StatefulFunctionsConfig statefunConfig = StatefulFunctionsConfig.fromEnvironment(env);
        statefunConfig.setFactoryType(MessageFactoryType.WITH_KRYO_PAYLOADS);
        statefunConfig.setMaxAsyncOperationsPerTask(CASSANDRA_CONCURRENT_REQUESTS);
        statefunConfig.setFlinkJobName(flinkJobName);
        StatefulFunctionEgressStreams out = StatefulFunctionDataStreamBuilder.builder("address_clustering")
                .withFunctionProvider(UnionFindFunction.TYPE, functionType -> new UnionFindFunction())
                .withDataStreamAsIngress(rms)
                .withConfiguration(statefunConfig)
                .withEgressId(UnionFindFunction.EGRESS)
                .build(env);
        
        DataStream<AddressOperation> cassandraAddressOperations = out.getDataStreamForEgressId(UnionFindFunction.EGRESS);
        
        
        UnionFindSink sink = new UnionFindSink(cassandraAddressOperations.getType().createSerializer(cassandraAddressOperations.getExecutionEnvironment().getConfig()), sessionBuilder);
        SingleOutputStreamOperator<AddressOperation> nothing = cassandraAddressOperations.transform("Scylla Sink", null, sink).uid("scylla_sink");
        
        env.execute(flinkJobName);
        //out.

    }
    
    private static boolean possiblyCoinJoin(ConfirmedTransactionWithInputs tx) {
        Set<String> inputAddresses = new HashSet<>();
        for (TransactionInputWithOutput vin : tx.getVin()) {
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
