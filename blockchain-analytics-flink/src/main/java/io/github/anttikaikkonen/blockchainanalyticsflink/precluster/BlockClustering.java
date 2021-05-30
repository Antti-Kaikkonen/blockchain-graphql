package io.github.anttikaikkonen.blockchainanalyticsflink.precluster;

import io.github.anttikaikkonen.blockchainanalyticsflink.statefun.unionfind.BlockTx;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class BlockClustering extends KeyedProcessFunction<Integer, Tuple2<Integer, SimpleAddAddressesAndTransactionsOperation[]>, Tuple2<Integer, SimpleAddAddressesAndTransactionsOperation[]>> {

    private MapState<String, String> persistedParent;
    private MapState<String, Integer> persistedSize;
    private MapState<String, HashMap<Integer, Long>> persistedTransactions;//Address -> TxN -> BalanceChange

    @Override
    public void open(Configuration parameters) throws Exception {
        this.persistedParent = getRuntimeContext().getMapState(new MapStateDescriptor<>("parent", String.class, String.class));
        this.persistedSize = getRuntimeContext().getMapState(new MapStateDescriptor<>("size", String.class, Integer.class));
        this.persistedTransactions = getRuntimeContext().getMapState(new MapStateDescriptor("transactions", TypeInformation.of(String.class), TypeInformation.of(new TypeHint<HashMap<Integer, Long>>() {
        })));
    }

    public void makeSet(String x) throws Exception {
        if (persistedParent.get(x) == null) {
            persistedParent.put(x, x);
            persistedSize.put(x, 1);
        }
    }

    public void addTx(String address, Integer txN, Long balanceChange) throws Exception {
        String root = this.find(address);

        HashMap<Integer, Long> oldValue = this.persistedTransactions.get(root);
        if (oldValue == null) {
            oldValue = new HashMap();
        }
        oldValue.merge(txN, balanceChange, (a, b) -> a + b);
        this.persistedTransactions.put(root, oldValue);
    }

    public void merge(SimpleAddAddressesAndTransactionsOperation[] b) throws Exception {
        for (SimpleAddAddressesAndTransactionsOperation op : b) {
            for (int i = 1; i < op.addresses.length; i++) {
                this.union(op.addresses[0], op.addresses[i]);
            }
            for (BlockTx blockTx : op.blockTxs) {
                this.addTx(op.addresses[0], blockTx.getTxN(), blockTx.getDelta());
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
            HashMap<Integer, Long> yTxs = this.persistedTransactions.get(y);
            this.persistedTransactions.remove(y);
            if (yTxs != null) {
                HashMap<Integer, Long> oldValue = this.persistedTransactions.get(x);
                if (oldValue == null) {
                    oldValue = yTxs;
                } else {
                    for (Map.Entry<Integer, Long> tx : yTxs.entrySet()) {
                        oldValue.merge(tx.getKey(), tx.getValue(), (a, b) -> a + b);
                    }
                }
                persistedTransactions.put(x, oldValue);
            }

            this.persistedSize.put(x, xSize + ySize);
        }
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

    public SimpleAddAddressesAndTransactionsOperation[] toAddOps() throws Exception {
        List<String> roots = new ArrayList();
        Map<String, List<String>> parentToChildren = new HashMap<>();
        for (final Map.Entry<String, String> kv : this.persistedParent.entries()) {
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
        SimpleAddAddressesAndTransactionsOperation[] res = new SimpleAddAddressesAndTransactionsOperation[roots.size()];
        int rootIndex = 0;
        for (String root : roots) {
            List<String> connectedAddresses = descendants(root, parentToChildren);
            connectedAddresses.add(root);
            Collections.sort(connectedAddresses);
            Map<Integer, Long> txs = this.persistedTransactions.get(root);
            BlockTx[] blockTxs = new BlockTx[txs.size()];
            int transactionIndex = 0;
            for (Map.Entry<Integer, Long> e : txs.entrySet()) {
                BlockTx blockTx = new BlockTx(e.getKey(), e.getValue());
                blockTxs[transactionIndex] = blockTx;
                transactionIndex++;
            }
            SimpleAddAddressesAndTransactionsOperation op = new SimpleAddAddressesAndTransactionsOperation(connectedAddresses.toArray(new String[connectedAddresses.size()]), blockTxs);
            res[rootIndex] = op;
            rootIndex++;
        }
        return res;
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<Integer, SimpleAddAddressesAndTransactionsOperation[]>> out) throws Exception {
        out.collect(new Tuple2<>(ctx.getCurrentKey(), toAddOps()));
        this.persistedSize.clear();
        this.persistedTransactions.clear();
        this.persistedParent.clear();
    }

    @Override
    public void processElement(Tuple2<Integer, SimpleAddAddressesAndTransactionsOperation[]> input, Context ctx, Collector<Tuple2<Integer, SimpleAddAddressesAndTransactionsOperation[]>> out) throws Exception {
        this.merge(input.f1);
        ctx.timerService().registerEventTimeTimer(ctx.timestamp());
    }

}
