package io.github.anttikaikkonen.blockchainanalyticsflink.statefun.unionfind;

import io.github.anttikaikkonen.blockchainanalyticsflink.statefun.cassandraexecutor.AddressOperation;
import io.github.anttikaikkonen.blockchainanalyticsflink.statefun.cassandraexecutor.DeleteCluster;
import io.github.anttikaikkonen.blockchainanalyticsflink.statefun.cassandraexecutor.SetBalanceOperation;
import io.github.anttikaikkonen.blockchainanalyticsflink.statefun.cassandraexecutor.SetDailyBalanceChange;
import io.github.anttikaikkonen.blockchainanalyticsflink.statefun.cassandraexecutor.SetParent;
import java.util.Iterator;
import java.util.Map;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.annotations.Persisted;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.state.PersistedTable;
import org.apache.flink.statefun.sdk.state.PersistedValue;

public class UnionFindFunction implements StatefulFunction {

    public static final EgressIdentifier<AddressOperation> EGRESS = new EgressIdentifier<>("address_clustering", "union_find_sink", AddressOperation.class);

    public static final FunctionType TYPE = new FunctionType("address_clustering", "union_find");

    @Persisted
    private final PersistedValue<Object> persistedParentOrSize = PersistedValue.of("parent", Object.class);//Parent address (String) or size (Long) if in the root node

    @Persisted
    private final PersistedTable<String, String> persistedAddresses = PersistedTable.of("addresses", String.class, String.class);

    @Persisted
    private final PersistedTable<TxPointer, Long> persistedTransactions = PersistedTable.of("transactions", TxPointer.class, Long.class);

    @Persisted
    private final PersistedTable<Integer, Long> persistedDailyBalanceChanges = PersistedTable.of("dailyBalanceChanges", Integer.class, Long.class);//Day -> balance_change

    @Persisted
    private final PersistedValue<Long> persistedClusterTransactionCount = PersistedValue.of("clusterTransactionCount", Long.class);

    @Persisted
    private final PersistedValue<Long> persistedBalance = PersistedValue.of("balance", Long.class);

    public UnionFindFunction() {
    }

    private void sendToCassandra(Context context, Object output) {
        AddressOperation op = new AddressOperation(context.self().id(), output);
        context.send(EGRESS, op);
    }

    public void handleAddAddressesAndTransactionsOperation(AddAddressesAndTransactionsOperation op, Context context) {
        Object parentOrSize = persistedParentOrSize.getOrDefault(1l);

        if (parentOrSize instanceof String) {
            String parent = (String) parentOrSize;
            if (context.caller() != null) {
                context.send(new Address(TYPE, context.caller().id()), new Compress(parent));
            }
            context.send(new Address(TYPE, parent), op);
        } else {
            int day = (int) (op.getTimestamp() / 1000 / 60 / 60 / 24);
            long oldTxCount = this.persistedClusterTransactionCount.getOrDefault(0l);
            long balanceChange = 0l;
            int newTransactions = 0;
            for (BlockTx tx : op.getBlockTxs()) {
                balanceChange += tx.getDelta();
                TxPointer key = new TxPointer(op.getTimestamp(), op.getHeight(), tx.getTxN());
                Long oldDelta = (oldTxCount == 0) ? null : persistedTransactions.get(key);
                if (oldDelta == null) {
                    newTransactions++;
                }
                long newDelta = oldDelta == null ? tx.getDelta() : tx.getDelta() + oldDelta;
                persistedTransactions.set(key, newDelta);
                sendToCassandra(context, new AddTransactionOperation(op.getTimestamp(), op.getHeight(), tx.getTxN(), newDelta));
            }
            if (newTransactions > 0) {
                this.persistedClusterTransactionCount.set(oldTxCount + newTransactions);
            }
            final long totalBalanceChange = balanceChange;
            final long newBalance;
            if (oldTxCount == 0) {
                this.persistedBalance.set(totalBalanceChange);
                newBalance = totalBalanceChange;
            } else {
                newBalance = this.persistedBalance.updateAndGet(oldBalance -> oldBalance == null ? totalBalanceChange : oldBalance + totalBalanceChange);
            }
            sendToCassandra(context, new SetBalanceOperation(newBalance));

            Long oldDayBalanceChange = oldTxCount == 0 ? null : this.persistedDailyBalanceChanges.get(day);
            long newDayBalanceChange = oldDayBalanceChange == null ? totalBalanceChange : oldDayBalanceChange + totalBalanceChange;
            this.persistedDailyBalanceChanges.set(day, newDayBalanceChange);
            sendToCassandra(context, new SetDailyBalanceChange(day, newDayBalanceChange));

            String address = context.self().id();
            for (String addr : op.getAddresses()) {
                if (addr.equals(address)) {
                    persistedAddresses.set("", "");
                } else {
                    persistedAddresses.set(addr, "");
                }
                sendToCassandra(context, new AddAddressOperation(addr));
            }
        }
    }

    @Override
    public void invoke(Context context, Object input) {
        if (input instanceof MergeOperation) {
            MergeOperation e = (MergeOperation) input;
            merge(context, e);
        } else if (input instanceof MergeRootOperation) {
            MergeRootOperation e = (MergeRootOperation) input;
            mergeRoot(context, e);
        } else if (input instanceof Compress) {
            Compress e = (Compress) input;
            compress(context, e);
        } else if (input instanceof SetSize) {
            SetSize e = (SetSize) input;
            setSize(context, e);
        } else if (input instanceof AddTransactionOperation) {
            AddTransactionOperation e = (AddTransactionOperation) input;
            addTransaction(context, e);
        } else if (input instanceof AddAddressOperation) {
            AddAddressOperation e = (AddAddressOperation) input;
            addAddress(context, e);
        } else if (input instanceof AddAddressesAndTransactionsOperation) {
            AddAddressesAndTransactionsOperation e = (AddAddressesAndTransactionsOperation) input;
            handleAddAddressesAndTransactionsOperation(e, context);
        } else if (input instanceof AddBalanceOperation) {
            AddBalanceOperation e = (AddBalanceOperation) input;
            addBalance(context, e);
        } else if (input instanceof AddDailyBalanceChangeOperation) {
            AddDailyBalanceChangeOperation e = (AddDailyBalanceChangeOperation) input;
            addDailyBalanceChange(context, e);
        } else {
            throw new RuntimeException("Unkown type!" + input.getClass().getName());
        }
    }

    private void merge(Context context, MergeOperation merge) {
        final String callerAddress;
        if (context.caller() != null && context.caller().type().equals(TYPE)) {
            callerAddress = context.caller().id();
        } else {
            callerAddress = null;
        }
        String address = context.self().id();
        Object parentOrSize = persistedParentOrSize.getOrDefault(1l);

        if (parentOrSize instanceof Long) {
            long size = (long) parentOrSize;
            //long size = persistentAddressCount.get();
            for (String from : merge.getFromAddresses()) {
                MergeRootOperation mergeRoot = new MergeRootOperation(address, size, false);
                context.send(new Address(TYPE, from), mergeRoot);
            }
        } else {
            String parent = (String) parentOrSize;
            if (callerAddress != null) {
                context.send(new Address(TYPE, callerAddress), new Compress(parent));
            }
            Iterator<String> fromAddresses = merge.getFromAddresses().iterator();
            while (fromAddresses.hasNext()) {
                String from = fromAddresses.next();
                if (parent.equals(from)) {//Already in the same set
                    fromAddresses.remove();
                }
            }
            if (merge.getFromAddresses().size() > 0) {
                context.send(new Address(TYPE, parent), merge);
            }
        }
    }

    private void mergeRoot(Context context, MergeRootOperation mergeRoot) {
        String address = context.self().id();
        Object parentOrSize = persistedParentOrSize.getOrDefault(1l);
        if (parentOrSize instanceof Long) {
            long size = (long) parentOrSize;
            if (mergeRoot.getRootSize() > size || (mergeRoot.getRootSize() == size && address.compareTo(mergeRoot.getRoot()) > 0)) {
                setParent(context, mergeRoot.getRoot());
                context.send(new Address(TYPE, mergeRoot.getRoot()), new SetSize(size, false));

                Long balanceToMerge = this.persistedBalance.get();
                if (balanceToMerge != null) {
                    context.send(new Address(TYPE, mergeRoot.getRoot()), new AddBalanceOperation(balanceToMerge));

                    Iterable<String> addresses = this.persistedAddresses.keys();
                    for (String a : addresses) {
                        if (a.equals("")) {
                            context.send(new Address(TYPE, mergeRoot.getRoot()), new AddAddressOperation(address));
                        } else {
                            context.send(new Address(TYPE, mergeRoot.getRoot()), new AddAddressOperation(a));
                        }
                    }

                    Iterable<Map.Entry<Integer, Long>> dailyBalanceChanges = persistedDailyBalanceChanges.entries();
                    for (Map.Entry<Integer, Long> e : dailyBalanceChanges) {
                        context.send(new Address(TYPE, mergeRoot.getRoot()), new AddDailyBalanceChangeOperation(e.getKey(), e.getValue()));
                    }
                    Iterable<Map.Entry<TxPointer, Long>> entries = persistedTransactions.entries();
                    for (Map.Entry<TxPointer, Long> e : entries) {
                        AddTransactionOperation addTx = new AddTransactionOperation(e.getKey().getTime(), e.getKey().getHeight(), e.getKey().getTx_n(), e.getValue());
                        context.send(new Address(TYPE, mergeRoot.getRoot()), addTx);
                    }
                    deleteCluster(context);

                }
            } else {
                context.send(new Address(TYPE, mergeRoot.getRoot()), new MergeRootOperation(address, size, false));//Merge from mergeRoot.getRoot() to this address
            }
        } else {
            String parent = (String) parentOrSize;
            if (parent.equals(mergeRoot.getRoot())) {//already in the same set
                if (mergeRoot.isCalledFromChild()) {
                    context.send(new Address(TYPE, context.caller().id()), new Compress(parent));
                }
            } else {
                if (mergeRoot.isCalledFromChild()) {
                    context.send(new Address(TYPE, context.caller().id()), new Compress(parent));
                }
                mergeRoot.setCalledFromChild(true);
                context.send(new Address(TYPE, parent), mergeRoot);
            }
        }
    }

    private void compress(Context context, Compress compress) {
        context.caller();
        String parent = (String) persistedParentOrSize.get();

        if (parent.equals(compress.getTo())) {
        } else if (parent.equals(context.caller().id())) {
            setParent(context, compress.getTo());
        } else {
        }
    }

    private void setSize(Context context, SetSize setSize) {
        Object parentOrSize = persistedParentOrSize.getOrDefault(1l);
        if (parentOrSize instanceof Long) {
            long oldSize = (long) parentOrSize;
            persistedParentOrSize.set(oldSize + setSize.getAdd());
        } else {
            String parent = (String) parentOrSize;
            if (setSize.isCalledFromChild()) {
                context.send(new Address(TYPE, context.caller().id()), new Compress(parent));
            }
            setSize.setCalledFromChild(true);
            context.send(new Address(TYPE, parent), setSize);
        }
    }

    public void addTransaction(Context context, AddTransactionOperation addTransactionOperation) {
        Object parentOrSize = persistedParentOrSize.getOrDefault(1l);

        if (parentOrSize instanceof Long) {
            long oldTxCount = persistedClusterTransactionCount.getOrDefault(0l);
            TxPointer key = new TxPointer(addTransactionOperation.getTime(), addTransactionOperation.getHeight(), addTransactionOperation.getTx_n());
            Long oldDelta = (oldTxCount == 0) ? null : persistedTransactions.get(key);
            long newDelta = oldDelta == null ? addTransactionOperation.getDelta() : addTransactionOperation.getDelta() + oldDelta;
            addTransactionOperation.setDelta(newDelta);
            persistedTransactions.set(key, newDelta);
            if (oldDelta == null) {
                persistedClusterTransactionCount.set(oldTxCount + 1);
            }
            sendToCassandra(context, addTransactionOperation);
        } else {
            String parent = (String) parentOrSize;
            context.send(new Address(TYPE, parent), addTransactionOperation);
        }
    }

    private void setParent(Context context, String parent) {
        persistedParentOrSize.set(parent);
        String address = context.self().id();
        if (address.equals(parent)) {
            sendToCassandra(context, new SetParent(null));
        } else {
            sendToCassandra(context, new SetParent(parent));
        }
    }

    private void addAddress(Context context, AddAddressOperation addAddressOp) {
        Object parentOrSize = persistedParentOrSize.getOrDefault(1l);
        String address = context.self().id();
        if (parentOrSize instanceof Long) {
            if (addAddressOp.getAddress().equals(address)) {
                persistedAddresses.set("", "");//keep state smaller
            } else {
                persistedAddresses.set(addAddressOp.getAddress(), "");
            }
            sendToCassandra(context, addAddressOp);
            //hasClusterAddresses.set(true);
        } else {
            String parent = (String) parentOrSize;
            context.send(new Address(TYPE, "" + parent), addAddressOp);
        }
    }

    private void deleteCluster(Context context) {
        persistedAddresses.clear();
        //hasClusterAddresses.clear();
        persistedTransactions.clear();
        persistedClusterTransactionCount.clear();
        persistedBalance.clear();

        persistedDailyBalanceChanges.clear();
        sendToCassandra(context, new DeleteCluster());
    }

    private void addBalance(Context context, AddBalanceOperation addBalanceOp) {
        Object parentOrSize = persistedParentOrSize.getOrDefault(1l);
        if (parentOrSize instanceof Long) {
            long newBalance = this.persistedBalance.updateAndGet(oldBalance -> oldBalance == null ? addBalanceOp.getBalance() : oldBalance + addBalanceOp.getBalance());
            sendToCassandra(context, new SetBalanceOperation(newBalance));
        } else {
            String parent = (String) parentOrSize;
            context.send(new Address(TYPE, "" + parent), addBalanceOp);
        }
    }

    private void addDailyBalanceChange(Context context, AddDailyBalanceChangeOperation addDailyBalanceChangeOp) {
        Object parentOrSize = persistedParentOrSize.getOrDefault(1l);
        if (parentOrSize instanceof Long) {
            Long oldBalanceChange = persistedDailyBalanceChanges.get(addDailyBalanceChangeOp.getEpochDate());
            long newBalanceChange = oldBalanceChange == null ? addDailyBalanceChangeOp.getBalanceChange() : oldBalanceChange + addDailyBalanceChangeOp.getBalanceChange();
            persistedDailyBalanceChanges.set(addDailyBalanceChangeOp.getEpochDate(), newBalanceChange);
            sendToCassandra(context, new SetDailyBalanceChange(addDailyBalanceChangeOp.getEpochDate(), newBalanceChange));
        } else {
            String parent = (String) parentOrSize;
            context.send(new Address(TYPE, "" + parent), addDailyBalanceChangeOp);
        }
    }

}
