package io.github.anttikaikkonen.blockchainanalyticsflink.source;

import io.github.anttikaikkonen.bitcoinrpcclientjava.models.BlockHeader;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class StrictlyIncreasingHeaderTimeAssigner extends ProcessFunction<BlockHeader, BlockHeader> implements CheckpointedFunction {

    private transient ListState<Long> checkpointedTime;
    private long time = 0;
    
    @Override
    public void processElement(BlockHeader header, Context ctx, Collector<BlockHeader> collector) throws Exception {
        if (header.getTime()*1000 > time) {
            time = header.getTime()*1000;
        } else {
            time = time+1;
        }
        header.setTime(time);
        collector.collect(header);
    }
    
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        this.checkpointedTime.clear();
        this.checkpointedTime.add(time);
        System.out.println("HeaderTimeProcessor snapshotState");
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        this.checkpointedTime = context.getOperatorStateStore().getListState(new ListStateDescriptor<>("time", Long.class));
        if (context.isRestored()) {
            for (Long time : this.checkpointedTime.get()) {
                this.time = time;
            }
        }
        System.out.println("HeaderTimeProcessor initializeState. Time = "+this.time);
    }

}
