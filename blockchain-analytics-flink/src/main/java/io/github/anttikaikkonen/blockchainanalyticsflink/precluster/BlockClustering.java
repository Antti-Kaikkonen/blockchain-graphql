package io.github.anttikaikkonen.blockchainanalyticsflink.precluster;

import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class BlockClustering extends KeyedProcessFunction<Integer, Tuple2<Integer, DisjointSetForest>, Tuple2<Integer, DisjointSetForest>> {
    
    private MapState<String, String> persistedParent;
    private MapState<String, Integer> persistedSize;
    private MapState<String, Map<Integer, Long>> persistedTransactions;//Address -> TxN -> BalanceChange    

    @Override
    public void open(Configuration parameters) throws Exception {
        this.persistedParent = getRuntimeContext().getMapState(new MapStateDescriptor<>("parent", String.class, String.class));
        this.persistedSize = getRuntimeContext().getMapState(new MapStateDescriptor<>("size", String.class, Integer.class));
        //MapStateDescriptor<String, Map<Integer, Long>> d = new MapStateDescriptor("", TypeInformation.of(String.class), TypeInformation.of(new TypeHint<Map<Integer, Long>>() {}));
        this.persistedTransactions = getRuntimeContext().getMapState(new MapStateDescriptor("transactions", TypeInformation.of(String.class), TypeInformation.of(new TypeHint<Map<Integer, Long>>() {})));
    }
    
    
    public void makeSet(String x) throws Exception {
        if (persistedParent.get(x) == null) {
            persistedParent.put(x, x);
            persistedSize.put(x, 1);
        }
    }
    
    public void addTx(String address, Integer txN, Long balanceChange) throws Exception {
        String root = this.find(address);
        
        Map<Integer, Long> oldValue = this.persistedTransactions.get(root);
        if (oldValue == null) {
            oldValue = new HashMap();
        }
        oldValue.merge(txN, balanceChange, (a, b) -> a+b);
        this.persistedTransactions.put(root, oldValue);
    }

    
    public void merge(DisjointSetForest b) throws Exception {
        for (Map.Entry<String, String> kv : b.parent.entrySet()) {
            if (!kv.getKey().equals(kv.getValue())) {
                this.union(kv.getKey(), kv.getValue());
            } else {
                this.makeSet(kv.getKey());
            }
        }
        for (Map.Entry<String, Map<Integer, Long>> addressTransactions : b.transactions.entrySet()) {
            String address = addressTransactions.getKey();
            for (Map.Entry<Integer, Long> addressTransaction : addressTransactions.getValue().entrySet()) {
                this.addTx(address, addressTransaction.getKey(), addressTransaction.getValue());
            }
        }
    }
    
    public String find(String x) throws Exception {
        String p = this.persistedParent.get(x);
        if (p == null) {
            makeSet(x);
            return x;
        } else if (!x.equals(p)) {
            String root = find(p);
            this.persistedParent.put(x, root);
            return root;
        } else {
            return x;
        }
    }


    public void union(String x, String y) throws Exception {
        x = find(x);
        y = find(y);
        if (x.equals(y)) {
            return;
        } else {
            Integer xSize = this.persistedSize.get(x);
            Integer ySize = this.persistedSize.get(y);
            if (xSize < ySize) {
                String tmp = x;
                x = y;
                y = tmp;
            }
            this.persistedParent.put(y, x);//merge y to x
            Map<Integer, Long> yTxs = this.persistedTransactions.get(y);
            this.persistedTransactions.remove(y);
            if (yTxs != null) {
                Map<Integer, Long> oldValue = this.persistedTransactions.get(x);
                if (oldValue == null) {
                    oldValue = yTxs;
                } else {
                    for (Map.Entry<Integer, Long> tx : yTxs.entrySet()) {
                        oldValue.merge(tx.getKey(), tx.getValue(), (a, b) -> a+b);
                    }
                }
                persistedTransactions.put(x, oldValue);
            }

            this.persistedSize.put(x, xSize+ySize);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<Integer, DisjointSetForest>> out) throws Exception {
        DisjointSetForest dsf = new DisjointSetForest();
        for (Map.Entry<String, String> e : this.persistedParent.entries()) {
            dsf.parent.put(e.getKey(), e.getValue());
        }
        this.persistedParent.clear();
        for (Map.Entry<String, Integer> e : this.persistedSize.entries()) {
            dsf.size.put(e.getKey(), e.getValue());
        }    
        this.persistedSize.clear();
        for (Map.Entry<String, Map<Integer, Long>> e : this.persistedTransactions.entries()) {
            dsf.transactions.put(e.getKey(), e.getValue());
        }
        this.persistedTransactions.clear();
        out.collect(new Tuple2<>(ctx.getCurrentKey(), dsf));
    }
    
    @Override
    public void processElement(Tuple2<Integer, DisjointSetForest> input, Context ctx, Collector<Tuple2<Integer, DisjointSetForest>> out) throws Exception {
        this.merge(input.f1);
        ctx.timerService().registerEventTimeTimer(ctx.timestamp());
    }

}
