package io.github.anttikaikkonen.blockchainanalyticsflink.statefun.sink;

import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.CassandraSessionBuilder;
import io.github.anttikaikkonen.blockchainanalyticsflink.statefun.cassandraexecutor.AddressOperation;
import io.github.anttikaikkonen.blockchainanalyticsflink.statefun.cassandraexecutor.SetParent;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class UnionFindSink extends KeyedProcessFunction<String, AddressOperation, Object> implements CheckpointedFunction, CheckpointListener {

    private static final int MAX_UNCOMPLETED_CHECKPOINTS = 3;
   
    private final CassandraSessionBuilder sessionBuilder;
    
    private Long completedCheckpoint = 0l;//0 = no checkpoints completed
    private transient ListState<Long> persistedCompletedCheckpoint;
    
    private ValueState<Long> addressFirstUncommittedCheckpoint;//earliest uncommitted address checkpoint
    
    private ValueState<Long> addressLastUncommittedCheckpoint;//last uncommitted address checkpoint
    
    private ListState<Object>[] checkpointedOperations;//Ring buffer containing up to MAX_UNCOMPLETED_CHECKPOINTS uncommitted checkpoints for an address
    
    private ValueState<SetParent>[] checkpointedSetParentOperations;
    
    private final long checkpointCheckInterval;
    
    public UnionFindSink(CassandraSessionBuilder sessionBuilder, long checkpointCheckInterval) {
        this.checkpointCheckInterval = checkpointCheckInterval;
        this.sessionBuilder = sessionBuilder;
    }

    /**
     * Registers keyed states and prepares cql statements
     * @param parameters
     * @throws Exception 
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        addressFirstUncommittedCheckpoint = getRuntimeContext().getState(new ValueStateDescriptor("fromCheckpoint", Long.class));
        addressLastUncommittedCheckpoint = getRuntimeContext().getState(new ValueStateDescriptor("toCheckpoint", Long.class));
        checkpointedOperations = new ListState[MAX_UNCOMPLETED_CHECKPOINTS];
        checkpointedSetParentOperations = new ValueState[MAX_UNCOMPLETED_CHECKPOINTS];
        for (int i = 0; i < MAX_UNCOMPLETED_CHECKPOINTS; i++) {
            checkpointedOperations[i] = getRuntimeContext().getListState(new ListStateDescriptor("checkpointedOperations"+i, Object.class));
            checkpointedSetParentOperations[i] = getRuntimeContext().getState(new ValueStateDescriptor("checkpointedSetParentOperations"+i, SetParent.class));
        }
        
    }
    
    
    private void flush(String address, long checkpointId, Collector<Object> out) throws Exception {
        ListState<Object> checkpointedOps = checkpointedOperations[(int)(checkpointId%MAX_UNCOMPLETED_CHECKPOINTS)];
        for (Object op2 : checkpointedOps.get()) {
            AddressOperation op = new AddressOperation(address, op2);
            out.collect(op);
        }
        checkpointedOps.clear();
        ValueState<SetParent> setParentOp = checkpointedSetParentOperations[(int)(checkpointId%MAX_UNCOMPLETED_CHECKPOINTS)];
        SetParent setParent = setParentOp.value();
        if (setParent != null) {
            AddressOperation op = new AddressOperation(address, setParent);
            out.collect(op);
            setParentOp.clear();
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Object> out) throws Exception {
        String address = ctx.getCurrentKey();
        long firstUncommittedCheckpoint = addressFirstUncommittedCheckpoint.value();
        long lastUncommittedCheckpoint = addressLastUncommittedCheckpoint.value();
        if (lastUncommittedCheckpoint-firstUncommittedCheckpoint >= MAX_UNCOMPLETED_CHECKPOINTS) {
            throw new Exception("Address "+address+" has more than the maximum allowed "+MAX_UNCOMPLETED_CHECKPOINTS+" uncommitted checkpoints");
        }
        if (lastUncommittedCheckpoint < completedCheckpoint) {
            for (long checkpointId = firstUncommittedCheckpoint; checkpointId <= lastUncommittedCheckpoint; checkpointId++) {
                flush(address, checkpointId, out);
            }
            addressFirstUncommittedCheckpoint.clear();
            addressLastUncommittedCheckpoint.clear();
        } else {
            for (long checkpointId = firstUncommittedCheckpoint; checkpointId < completedCheckpoint; checkpointId++) {
                flush(address, checkpointId, out);
                addressFirstUncommittedCheckpoint.update(checkpointId+1);
            }
            ctx.timerService().registerProcessingTimeTimer(timestamp+checkpointCheckInterval);
        }

    }
    
    @Override
    public void processElement(AddressOperation input, Context ctx, Collector<Object> out) throws Exception {
        addressLastUncommittedCheckpoint.update(completedCheckpoint);
        if (addressFirstUncommittedCheckpoint.value() == null) {
            addressFirstUncommittedCheckpoint.update(completedCheckpoint);
            //long millisToNextMinute = ctx.timerService().currentProcessingTime()%this.checkpointCheckInterval;
            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime()+this.checkpointCheckInterval);
        } 
        if (input.getOp() instanceof SetParent) {
            ValueState<SetParent> checkpointedOps = this.checkpointedSetParentOperations[(int)(completedCheckpoint%MAX_UNCOMPLETED_CHECKPOINTS)];
            SetParent setParent = (SetParent) input.getOp();
            checkpointedOps.update(setParent);
        } else {
            ListState<Object> checkpointedOps = this.checkpointedOperations[(int)(completedCheckpoint%MAX_UNCOMPLETED_CHECKPOINTS)];
            checkpointedOps.add(input.getOp());
        }
        
    }

    @Override
    public void snapshotState(FunctionSnapshotContext ctx) throws Exception {
        System.out.println("UnionFindSink snapshotState"+ctx.getCheckpointId());
        this.persistedCompletedCheckpoint.clear();
        this.persistedCompletedCheckpoint.add(ctx.getCheckpointId());
    }

    @Override
    public void initializeState(FunctionInitializationContext ctx) throws Exception {
        System.out.println("InitializeState UnionFindSink");
        this.persistedCompletedCheckpoint = ctx.getOperatorStateStore().getListState(new ListStateDescriptor("completedCheckpoint", Long.class));
        if (ctx.isRestored()) {
            System.out.println("UnionFindSink restoring state");
            for (Long checkpoinintId : this.persistedCompletedCheckpoint.get()) {
                this.completedCheckpoint = checkpoinintId;
            }
        }
    }
    
    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        System.out.println("UnionFindSink notifyCheckpointComplete"+checkpointId);
        this.completedCheckpoint = checkpointId;
        
    }

    
}
