package io.github.anttikaikkonen.blockchainanalyticsflink.precluster;

import io.github.anttikaikkonen.blockchainanalyticsflink.statefun.unionfind.AddAddressesAndTransactionsOperation;
import io.github.anttikaikkonen.blockchainanalyticsflink.statefun.unionfind.MergeOperation;
import io.github.anttikaikkonen.blockchainanalyticsflink.statefun.unionfind.UnionFindFunction;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.statefun.flink.core.message.RoutableMessage;
import org.apache.flink.statefun.flink.core.message.RoutableMessageBuilder;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class BlockClusterProcessor extends KeyedProcessFunction<Integer, Tuple2<Integer, SimpleAddAddressesAndTransactionsOperation[]>, RoutableMessage>  {

    private ListState<SimpleAddAddressesAndTransactionsOperation> persistedOps;
    private ValueState<Long> persistedTime;
    
    
    @Override
    public void open(Configuration parameters) throws Exception {
        this.persistedOps = getRuntimeContext().getListState(new ListStateDescriptor<>("ops", SimpleAddAddressesAndTransactionsOperation.class));
        this.persistedTime = getRuntimeContext().getState(new ValueStateDescriptor("time", Long.class));
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<RoutableMessage> out) throws Exception {
        Long time = this.persistedTime.value();
        Integer height = ctx.getCurrentKey();
        for (SimpleAddAddressesAndTransactionsOperation op : this.persistedOps.get()) {
            AddAddressesAndTransactionsOperation finalOp = new AddAddressesAndTransactionsOperation(op.getAddresses(), height, time, op.getBlockTxs());
            RoutableMessage rm = RoutableMessageBuilder.builder().withTargetAddress(new Address(UnionFindFunction.TYPE, op.getAddresses()[0])).withMessageBody(finalOp).build();
            out.collect(rm);
        }
        this.persistedOps.clear();
        this.persistedTime.clear();
    }
    
    private static List<String> descendants(String root, Map<String, List<String>> parentToChildren) {
        List<String> children = parentToChildren.get(root);
        ArrayList<String> all = new ArrayList<>();
        if (children != null) {
            for (String child : children) {
                all.add(child);
                all.addAll(descendants(child, parentToChildren));
            }
        }
        return all;
    }
    
    @Override
    public void processElement(Tuple2<Integer, SimpleAddAddressesAndTransactionsOperation[]> blockCluster, Context ctx, Collector<RoutableMessage> out) throws Exception {
        
        for (SimpleAddAddressesAndTransactionsOperation op : blockCluster.f1) {
            this.persistedOps.add(op);
            if (op.addresses.length > 1) {
                String toAddress = op.addresses[0];
                ArrayList<String> fromAddresses = new ArrayList<>();
                for (int i = 1; i < op.addresses.length; i++) {
                    fromAddresses.add(op.addresses[i]);
                }
                MergeOperation mergeOp = new MergeOperation(fromAddresses);
                RoutableMessage rm = RoutableMessageBuilder.builder().withTargetAddress(new Address(UnionFindFunction.TYPE, toAddress)).withMessageBody(mergeOp).build();
                out.collect(rm);
            }
        }
        /*
        List<String> roots = new ArrayList();
        Map<String, List<String>> parentToChildren = new HashMap<>();
        for (final Map.Entry<String, String> kv : blockCluster.f1.parent.entrySet()) {
            String child = kv.getKey();
            String parent = kv.getValue();
            if (child.equals(parent)) {
                roots.add(parent);
            }
            parentToChildren.compute(parent, new BiFunction<String, List<String>, List<String>>() {
                @Override
                public List<String> apply(String parent, List<String> oldValue) {
                    if (oldValue == null) {
                        oldValue = new ArrayList();
                    }
                    if (!parent.equals(child)) {
                        oldValue.add(child);
                    }
                    return oldValue;
                }
            });
        }
        for (String root : roots) {
            List<String> connectedAddresses = descendants(root, parentToChildren);
            connectedAddresses.add(root);
            Collections.sort(connectedAddresses);
            Map<Integer, Long> txs = blockCluster.f1.transactions.get(root);
            BlockTx[] blockTxs = new BlockTx[txs.size()];
            int transactionIndex = 0;
            for (Map.Entry<Integer, Long> e : txs.entrySet()) {
                BlockTx blockTx = new BlockTx(e.getKey(), e.getValue());
                blockTxs[transactionIndex] = blockTx;
                transactionIndex++;
            }
            SimpleAddAddressesAndTransactionsOperation op = new SimpleAddAddressesAndTransactionsOperation(connectedAddresses.toArray(new String[connectedAddresses.size()]), blockTxs);
            this.persistedOps.add(op);
            if (connectedAddresses.size() > 1) {
                String toAddress = connectedAddresses.get(0);
                ArrayList<String> fromAddresses = new ArrayList<>();
                for (int i = 1; i < connectedAddresses.size(); i++) {
                    fromAddresses.add(connectedAddresses.get(i));
                }
                MergeOperation mergeOp = new MergeOperation(fromAddresses);
                RoutableMessage rm = RoutableMessageBuilder.builder().withTargetAddress(new Address(UnionFindFunction.TYPE, toAddress)).withMessageBody(mergeOp).build();
                out.collect(rm);
            }
        }
        */
        
        persistedTime.update(ctx.timestamp());
        long currentTime = System.currentTimeMillis();
        long ago = currentTime-ctx.timestamp();
        long processTime = ctx.timestamp()+Math.round(ago*0.5);
        if (currentTime-processTime < Duration.ofDays(1).toMillis()) {//Process the last 24 hours in real-time
            ctx.timerService().registerEventTimeTimer(Math.max(ctx.timestamp(), currentTime-Duration.ofDays(1).toMillis()));
        } else {
            ctx.timerService().registerEventTimeTimer(processTime);
        }
    }


}
