package io.github.anttikaikkonen.blockchainanalyticsflink;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.CassandraSessionBuilder;
import io.github.anttikaikkonen.bitcoinrpcclientjava.RpcClient;
import io.github.anttikaikkonen.bitcoinrpcclientjava.models.BlockHeader;
import lombok.Builder;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

public class HeadersSource extends RichSourceFunction<BlockHeader> implements CheckpointedFunction {
    private static final int CONCURRENT_BLOCKS = 30;
    
    private volatile boolean isRunning = true;
    
    private transient ListState<Integer> checkpointedHeight;
    private int height = 0;
    private transient ListState<Long> checkpointedTime;
    private long time = 0l;
    
    private final int minConfirmations;
    private final long pollingInterval;
    
    private final RpcClientBuilder rpcClientBuilder;
    private final CassandraSessionBuilder sessionBuilder;
    private transient RpcClient rpcClient;
    private transient Session session;
    private PreparedStatement heightStatement;
    
    @Builder()
    public HeadersSource(Integer minConfirmations, Long pollingInterval, RpcClientBuilder rpcClientBuilder, CassandraSessionBuilder sessionBuilder) {
        this.sessionBuilder = sessionBuilder;
        this.minConfirmations = minConfirmations == null ? 5 : minConfirmations;
        this.pollingInterval = pollingInterval == null ? 1000l : pollingInterval;
        this.rpcClientBuilder = rpcClientBuilder;
    }
    
    @Override
    public void run(SourceContext<BlockHeader> ctx) throws Exception {
        System.out.println("Headers source run() height = "+this.height);
        String currentHash = this.rpcClient.getBlockHash(this.height).toCompletableFuture().get();
        while (this.isRunning) {
            long blockCount = this.rpcClient.getBlockCount().toCompletableFuture().get();
            long targetHeight = blockCount-this.minConfirmations;
            if (this.height <= targetHeight) {
                while (this.height <= targetHeight && this.isRunning) {
                    if (this.height%5 == 0 && this.height-CONCURRENT_BLOCKS >= CONCURRENT_BLOCKS) {
                        ResultSet res = this.session.execute(this.heightStatement.bind(this.height-CONCURRENT_BLOCKS));
                        if (res.one() == null) {
                            Thread.sleep(10);
                            continue;
                        }
                    }
                    BlockHeader header = this.rpcClient.getBlockHeader(currentHash).toCompletableFuture().get();
                    synchronized (ctx.getCheckpointLock()) {
                        if (header.getTime()*1000 > time) {
                            time = header.getTime()*1000;
                        } else {
                            time = time+1;
                        }
                        header.setTime(time);
                        ctx.collectWithTimestamp(header, time);
                        ctx.emitWatermark(new Watermark(time));
                        this.height++;
                        currentHash = header.getNextblockhash();
                    }
                }
            } else {
                Thread.sleep(this.pollingInterval);
            }
        }
        System.out.println("Headers source exit run");
    }

    @Override
    public void cancel() {
        System.out.println("HeadersSource cancel");
        this.isRunning = false;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        System.out.println("Block headers source: snapshotting state "+context.getCheckpointId());
        this.checkpointedHeight.clear();
        this.checkpointedHeight.add(height);
        this.checkpointedTime.clear();
        this.checkpointedTime.add(time);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        System.out.println("Block headers source: initialize state");
        this.checkpointedHeight = context.getOperatorStateStore().getListState(new ListStateDescriptor<>("height", Integer.class));
        this.checkpointedTime = context.getOperatorStateStore().getListState(new ListStateDescriptor<>("time", Long.class));
        if (context.isRestored()) {
            for (Integer height : this.checkpointedHeight.get()) {
                this.height = height;
            }
            for (Long time : this.checkpointedTime.get()) {
                this.time = time;
            }
        }
    }

    @Override
    public void close() throws Exception {
        System.out.println("HeadersSource close");
        //this.globalDao = null;
        if (this.session != null) {
            System.out.println("closing cluster");
            this.session.getCluster().close();
        }
        this.session = null;
        this.rpcClient = null;
        this.heightStatement = null;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("HeadersSource open");
        this.rpcClient = rpcClientBuilder.build();
        this.session = sessionBuilder.build();
        this.heightStatement = this.session.prepare("SELECT hash FROM longest_chain WHERE height = :height");
        
        //GlobalMapper mapper = new GlobalMapperBuilder(session)
        //        .build();
        
        //this.globalDao = mapper.createDAO(session.getKeyspace().get());
        getRuntimeContext().getMetricGroup().gauge("Current height", new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return height;
            }
        });
    }

}
