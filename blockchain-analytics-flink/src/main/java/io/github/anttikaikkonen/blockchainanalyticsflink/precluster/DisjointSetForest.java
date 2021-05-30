package io.github.anttikaikkonen.blockchainanalyticsflink.precluster;

import io.github.anttikaikkonen.blockchainanalyticsflink.statefun.unionfind.BlockTx;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

public class DisjointSetForest {

    public Map<String, String> parent = new HashMap();
    public Map<String, Integer> size = new HashMap();
    public Map<String, Map<Integer, Long>> transactions = new HashMap();//Address -> TxN -> BalanceChange

    public void makeSet(String x) {
        if (!parent.containsKey(x)) {
            parent.put(x, x);
            size.put(x, 1);
        }
    }

    public void merge(DisjointSetForest b) {
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

    public void addTx(String address, Integer txN, Long balanceChange) {
        String root = find(address);
        transactions.compute(root, new BiFunction<String, Map<Integer, Long>, Map<Integer, Long>>() {
            @Override
            public Map<Integer, Long> apply(String address, Map<Integer, Long> oldValue) {
                if (oldValue == null) {
                    oldValue = new HashMap();
                }
                oldValue.merge(txN, balanceChange, (a, b) -> a + b);
                return oldValue;
            }
        });
    }

    public String find(String x) {
        String p = parent.get(x);
        if (p == null) {
            makeSet(x);
            return x;
        } else if (!x.equals(p)) {
            String root = find(p);
            parent.put(x, root);
            return root;
        } else {
            return x;
        }
    }

    public void union(String x, String y) {
        x = find(x);
        y = find(y);
        if (x.equals(y)) {
            return;
        } else {
            Integer xSize = size.get(x);
            Integer ySize = size.get(y);
            if (xSize < ySize) {
                String tmp = x;
                x = y;
                y = tmp;
            }
            parent.put(y, x);//merge y to x
            Map<Integer, Long> yTxs = transactions.remove(y);
            if (yTxs != null) {
                transactions.compute(x, (String key, Map<Integer, Long> oldValue) -> {
                    if (oldValue == null) {
                        oldValue = yTxs;
                    } else {
                        for (Map.Entry<Integer, Long> tx : yTxs.entrySet()) {
                            oldValue.merge(tx.getKey(), tx.getValue(), (a, b) -> a + b);
                        }
                    }
                    return oldValue;
                });
            }

            size.put(x, xSize + ySize);
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

    public SimpleAddAddressesAndTransactionsOperation[] toAddOps() {
        List<String> roots = new ArrayList();
        Map<String, List<String>> parentToChildren = new HashMap<>();
        for (final Map.Entry<String, String> kv : this.parent.entrySet()) {
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
            Map<Integer, Long> txs = this.transactions.get(root);
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

}
