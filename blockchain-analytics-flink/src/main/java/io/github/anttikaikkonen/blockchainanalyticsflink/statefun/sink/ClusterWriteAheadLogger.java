package io.github.anttikaikkonen.blockchainanalyticsflink.statefun.sink;

import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.CassandraSessionBuilder;
import io.github.anttikaikkonen.blockchainanalyticsflink.statefun.cassandraexecutor.AddressOperation;
import io.github.anttikaikkonen.blockchainanalyticsflink.statefun.cassandraexecutor.DeleteCluster;
import io.github.anttikaikkonen.blockchainanalyticsflink.statefun.cassandraexecutor.SetBalanceOperation;
import io.github.anttikaikkonen.blockchainanalyticsflink.statefun.cassandraexecutor.SetDailyBalanceChange;
import io.github.anttikaikkonen.blockchainanalyticsflink.statefun.cassandraexecutor.SetParent;
import io.github.anttikaikkonen.blockchainanalyticsflink.statefun.unionfind.AddAddressOperation;
import io.github.anttikaikkonen.blockchainanalyticsflink.statefun.unionfind.AddTransactionOperation;
import io.github.anttikaikkonen.blockchainanalyticsflink.statefun.unionfind.TxPointer;
import java.util.Map;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class ClusterWriteAheadLogger extends KeyedProcessFunction<String, AddressOperation, Object> implements CheckpointedFunction, CheckpointListener {

    private static final int MAX_UNCOMPLETED_CHECKPOINTS = 3;

    private final CassandraSessionBuilder sessionBuilder;

    private Long completedCheckpoint = 0l;//0 = no checkpoints completed
    private transient ListState<Long> persistedCompletedCheckpoint;

    private ValueState<Long> addressFirstUncommittedCheckpoint;

    private ValueState<Long> addressLastUncommittedCheckpoint;

    private ValueState<DeleteCluster>[] checkpointedDeleteOperations;//Ring buffer containing up to MAX_UNCOMPLETED_CHECKPOINTS uncommitted checkpoints for an address

    private MapState<String, String>[] checkpointedAddAddressOperations;

    private MapState<TxPointer, Long>[] checkpointedAddTransactionOperations;

    private ValueState<SetParent>[] checkpointedSetParentOperations;

    private ValueState<Long>[] checkpointedSetBalanceOperation;

    private MapState<Integer, Long>[] checkpointedDailyBalanceChangeOpearations;

    private final long checkpointCheckInterval;

    public ClusterWriteAheadLogger(CassandraSessionBuilder sessionBuilder, long checkpointCheckInterval) {
        this.checkpointCheckInterval = checkpointCheckInterval;
        this.sessionBuilder = sessionBuilder;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        addressFirstUncommittedCheckpoint = getRuntimeContext().getState(new ValueStateDescriptor("fromCheckpoint", Long.class));
        addressLastUncommittedCheckpoint = getRuntimeContext().getState(new ValueStateDescriptor("toCheckpoint", Long.class));
        checkpointedDeleteOperations = new ValueState[MAX_UNCOMPLETED_CHECKPOINTS];
        checkpointedAddAddressOperations = new MapState[MAX_UNCOMPLETED_CHECKPOINTS];
        checkpointedAddTransactionOperations = new MapState[MAX_UNCOMPLETED_CHECKPOINTS];
        checkpointedSetParentOperations = new ValueState[MAX_UNCOMPLETED_CHECKPOINTS];
        checkpointedSetBalanceOperation = new ValueState[MAX_UNCOMPLETED_CHECKPOINTS];
        checkpointedDailyBalanceChangeOpearations = new MapState[MAX_UNCOMPLETED_CHECKPOINTS];
        for (int i = 0; i < MAX_UNCOMPLETED_CHECKPOINTS; i++) {
            checkpointedDeleteOperations[i] = getRuntimeContext().getState(new ValueStateDescriptor("checkpointedDeleteOperations" + i, DeleteCluster.class));
            checkpointedSetParentOperations[i] = getRuntimeContext().getState(new ValueStateDescriptor("checkpointedSetParentOperations" + i, SetParent.class));
            checkpointedAddAddressOperations[i] = getRuntimeContext().getMapState(new MapStateDescriptor("checkpointedAddAddressOperations" + i, String.class, String.class));
            checkpointedAddTransactionOperations[i] = getRuntimeContext().getMapState(new MapStateDescriptor("checkpointedAddTransactionOperations" + i, TxPointer.class, Long.class));
            checkpointedSetBalanceOperation[i] = getRuntimeContext().getState(new ValueStateDescriptor("checkpointedSetBalanceOperations" + i, Long.class));
            checkpointedDailyBalanceChangeOpearations[i] = getRuntimeContext().getMapState(new MapStateDescriptor("checkpointedDailyBalanceChangeOpearations" + i, Integer.class, Long.class));
        }

    }

    private void flushAddresses(String clusterId, long checkpointId, Collector<Object> out) throws Exception {
        MapState<String, String> addAddressOps = checkpointedAddAddressOperations[(int) (checkpointId % MAX_UNCOMPLETED_CHECKPOINTS)];
        int count = 0;
        for (String addAddress : addAddressOps.keys()) {
            AddAddressOperation addAddressOp = new AddAddressOperation(addAddress);
            AddressOperation op = new AddressOperation(clusterId, addAddressOp);
            out.collect(op);
            count++;
        }
        if (count > 0) {
            addAddressOps.clear();
        }
    }

    private void flushTransactions(String clusterId, long checkpointId, Collector<Object> out) throws Exception {
        MapState<TxPointer, Long> addTransactionOps = checkpointedAddTransactionOperations[(int) (checkpointId % MAX_UNCOMPLETED_CHECKPOINTS)];
        int count = 0;
        for (Map.Entry<TxPointer, Long> e : addTransactionOps.entries()) {
            AddTransactionOperation addTransactionOp = new AddTransactionOperation(e.getKey().getTime(), e.getKey().getHeight(), e.getKey().getTx_n(), e.getValue());
            AddressOperation op = new AddressOperation(clusterId, addTransactionOp);
            out.collect(op);
            count++;
        }
        if (count > 0) {
            addTransactionOps.clear();
        }
    }

    private void flushDailyBalanceChanges(String clusterId, long checkpointId, Collector<Object> out) throws Exception {
        MapState<Integer, Long> dailyBalanceChangeOps = checkpointedDailyBalanceChangeOpearations[(int) (checkpointId % MAX_UNCOMPLETED_CHECKPOINTS)];
        int count = 0;
        for (Map.Entry<Integer, Long> e : dailyBalanceChangeOps.entries()) {
            out.collect(new AddressOperation(clusterId, new SetDailyBalanceChange(e.getKey(), e.getValue())));
            count++;
        }
        if (count > 0) {
            dailyBalanceChangeOps.clear();
        }
    }

    private void flushDeleteOperations(String clusterId, long checkpointId, Collector<Object> out) throws Exception {
        ValueState<DeleteCluster> deleteOp = checkpointedDeleteOperations[(int) (checkpointId % MAX_UNCOMPLETED_CHECKPOINTS)];
        DeleteCluster deleteCluster = deleteOp.value();
        if (deleteCluster != null) {
            out.collect(new AddressOperation(clusterId, deleteCluster));
            deleteOp.clear();
        }
    }

    private void flush(String clusterId, long checkpointId, Collector<Object> out) throws Exception {

        flushAddresses(clusterId, checkpointId, out);
        flushTransactions(clusterId, checkpointId, out);
        flushDailyBalanceChanges(clusterId, checkpointId, out);
        flushDeleteOperations(clusterId, checkpointId, out);
        ValueState<SetParent> setParentOp = checkpointedSetParentOperations[(int) (checkpointId % MAX_UNCOMPLETED_CHECKPOINTS)];
        SetParent setParent = setParentOp.value();
        if (setParent != null) {
            AddressOperation op = new AddressOperation(clusterId, setParent);
            out.collect(op);
            setParentOp.clear();
        }
        ValueState<Long> setBalanceOp = checkpointedSetBalanceOperation[(int) (checkpointId % MAX_UNCOMPLETED_CHECKPOINTS)];
        Long balance = setBalanceOp.value();
        if (balance != null) {
            AddressOperation op = new AddressOperation(clusterId, new SetBalanceOperation(balance));
            out.collect(op);
            setBalanceOp.clear();
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Object> out) throws Exception {
        String address = ctx.getCurrentKey();
        long firstUncommittedCheckpoint = addressFirstUncommittedCheckpoint.value();
        long lastUncommittedCheckpoint = addressLastUncommittedCheckpoint.value();
        if (lastUncommittedCheckpoint - firstUncommittedCheckpoint >= MAX_UNCOMPLETED_CHECKPOINTS) {
            throw new Exception("Address " + address + " has more than the maximum allowed " + MAX_UNCOMPLETED_CHECKPOINTS + " uncommitted checkpoints. first: " + firstUncommittedCheckpoint + ", last: " + lastUncommittedCheckpoint);
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
            }
            addressFirstUncommittedCheckpoint.update(completedCheckpoint);
            ctx.timerService().registerEventTimeTimer(timestamp + checkpointCheckInterval);
        }
    }

    @Override
    public void processElement(AddressOperation input, Context ctx, Collector<Object> out) throws Exception {
        addressLastUncommittedCheckpoint.update(completedCheckpoint);
        if (addressFirstUncommittedCheckpoint.value() == null) {
            addressFirstUncommittedCheckpoint.update(completedCheckpoint);
            ctx.timerService().registerEventTimeTimer(ctx.timestamp() + checkpointCheckInterval);
        }
        if (input.getOp() instanceof SetParent) {
            ValueState<SetParent> checkpointedOps = this.checkpointedSetParentOperations[(int) (completedCheckpoint % MAX_UNCOMPLETED_CHECKPOINTS)];
            SetParent setParent = (SetParent) input.getOp();
            checkpointedOps.update(setParent);
        } else if (input.getOp() instanceof AddAddressOperation) {
            AddAddressOperation addAddressOp = (AddAddressOperation) input.getOp();
            MapState<String, String> state = checkpointedAddAddressOperations[(int) (completedCheckpoint % MAX_UNCOMPLETED_CHECKPOINTS)];
            state.put(addAddressOp.getAddress(), "");
        } else if (input.getOp() instanceof AddTransactionOperation) {
            AddTransactionOperation op = (AddTransactionOperation) input.getOp();
            MapState<TxPointer, Long> state = checkpointedAddTransactionOperations[(int) (completedCheckpoint % MAX_UNCOMPLETED_CHECKPOINTS)];
            state.put(new TxPointer(op.getTime(), op.getHeight(), op.getTx_n()), op.getDelta());
        } else if (input.getOp() instanceof DeleteCluster) {
            MapState<String, String> addAddressOps = checkpointedAddAddressOperations[(int) (completedCheckpoint % MAX_UNCOMPLETED_CHECKPOINTS)];
            addAddressOps.clear();
            MapState<TxPointer, Long> addTransactionOps = checkpointedAddTransactionOperations[(int) (completedCheckpoint % MAX_UNCOMPLETED_CHECKPOINTS)];
            addTransactionOps.clear();
            ValueState<Long> setBalanceOp = checkpointedSetBalanceOperation[(int) (completedCheckpoint % MAX_UNCOMPLETED_CHECKPOINTS)];
            setBalanceOp.clear();
            ValueState<DeleteCluster> deleteOp = this.checkpointedDeleteOperations[(int) (completedCheckpoint % MAX_UNCOMPLETED_CHECKPOINTS)];
            deleteOp.update((DeleteCluster) input.getOp());
        } else if (input.getOp() instanceof SetBalanceOperation) {
            ValueState<Long> setBalanceOp = checkpointedSetBalanceOperation[(int) (completedCheckpoint % MAX_UNCOMPLETED_CHECKPOINTS)];
            SetBalanceOperation op = (SetBalanceOperation) input.getOp();
            setBalanceOp.update(op.getBalance());
        } else if (input.getOp() instanceof SetDailyBalanceChange) {
            MapState<Integer, Long> dailyBalanceChangeOps = checkpointedDailyBalanceChangeOpearations[(int) (completedCheckpoint % MAX_UNCOMPLETED_CHECKPOINTS)];
            SetDailyBalanceChange op = (SetDailyBalanceChange) input.getOp();
            dailyBalanceChangeOps.put(op.getEpochDate(), op.getBalanceChange());
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext ctx) throws Exception {
        System.out.println("ClusterWriteAheadLogger snapshotState" + ctx.getCheckpointId());
        this.persistedCompletedCheckpoint.clear();
        this.persistedCompletedCheckpoint.add(ctx.getCheckpointId());
    }

    @Override
    public void initializeState(FunctionInitializationContext ctx) throws Exception {
        System.out.println("InitializeState ClusterWriteAheadLogger");
        this.persistedCompletedCheckpoint = ctx.getOperatorStateStore().getListState(new ListStateDescriptor("completedCheckpoint", Long.class));
        if (ctx.isRestored()) {
            for (Long checkpoinintId : this.persistedCompletedCheckpoint.get()) {
                this.completedCheckpoint = checkpoinintId;
            }
            System.out.println("ClusterWriteAheadLogger restoring state. Last completed checkpoint = " + this.completedCheckpoint);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        System.out.println("ClusterWriteAheadLogger notifyCheckpointComplete" + checkpointId);
        this.completedCheckpoint = checkpointId;

    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
    }

}
