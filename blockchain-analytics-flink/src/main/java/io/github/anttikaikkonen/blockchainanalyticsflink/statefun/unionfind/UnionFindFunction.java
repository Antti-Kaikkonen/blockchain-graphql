package io.github.anttikaikkonen.blockchainanalyticsflink.statefun.unionfind;


import io.github.anttikaikkonen.blockchainanalyticsflink.statefun.cassandraexecutor.AddressOperation;
import io.github.anttikaikkonen.blockchainanalyticsflink.statefun.cassandraexecutor.SetParent;
import java.util.Map;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.annotations.Persisted;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.state.PersistedTable;
import org.apache.flink.statefun.sdk.state.PersistedValue;
import io.github.anttikaikkonen.blockchainanalyticsflink.statefun.cassandraexecutor.DeleteTransactions;
import io.github.anttikaikkonen.blockchainanalyticsflink.statefun.cassandraexecutor.DeleteAddresses;
import java.util.Iterator;

public class UnionFindFunction implements StatefulFunction {
    
    public static final EgressIdentifier<AddressOperation> EGRESS = new EgressIdentifier<>("address_clustering", "union_find_sink", AddressOperation.class);

    public static final FunctionType TYPE = new FunctionType("address_clustering", "union_find");
    
    @Persisted
    private final PersistedValue<String> persistedParent = PersistedValue.of("parent", String.class);
    
    @Persisted
    private final PersistedValue<Long> persistentAddressCount = PersistedValue.of("addressCount", Long.class);
    
    @Persisted
    private final PersistedTable<String, String> persistedAddresses = PersistedTable.of("addresses", String.class, String.class);

    @Persisted
    private final PersistedTable<TxPointer, Long> persistedTransactions = PersistedTable.of("transactions", TxPointer.class, Long.class);
    
    @Persisted
    private final PersistedValue<Long> persistedClusterTransactionCount = PersistedValue.of("clusterTransactionCount", Long.class);
    
    @Persisted
    private final PersistedValue<Boolean> hasClusterAddresses = PersistedValue.of("hasClusterAddresses", Boolean.class);
    
    public UnionFindFunction() {
    }
    
    private void sendToCassandra(Context context, Object output) {
        AddressOperation op = new AddressOperation(context.self().id(), output);
        context.send(EGRESS, op);
    }
    
    public void handleAddAddressesAndTransactionsOperation(AddAddressesAndTransactionsOperation op, Context context) {
        String address = context.self().id();
        String parent = persistedParent.get();
        boolean makeSet = false;
        if (parent == null) {//Makeset
            makeset(context);
            parent = address;
            makeSet = true;
        }
        if (!address.equals(parent)) {
            if (context.caller() != null) {
                context.send(new Address(TYPE, context.caller().id()), new Compress(parent));
            }
            context.send(new Address(TYPE, parent), op);
        } else {
            long oldTxCount = makeSet ? 0 : this.persistedClusterTransactionCount.getOrDefault(0l);
            int newTransactions = 0;
            for (BlockTx tx : op.getBlockTxs()) {
                TxPointer key = new TxPointer(op.getTimestamp(), op.getHeight(), tx.getTxN());
                Long oldDelta = (oldTxCount == 0) ? null : persistedTransactions.get(key);
                if (oldDelta == null) newTransactions++;
                long newDelta = oldDelta == null ? tx.getDelta() : tx.getDelta()+oldDelta;
                persistedTransactions.set(key, newDelta);
                sendToCassandra(context, new AddTransactionOperation(op.getTimestamp(), op.getHeight(), tx.getTxN(), newDelta));
            }
            if (newTransactions > 0) this.persistedClusterTransactionCount.set(oldTxCount+newTransactions);
            

            hasClusterAddresses.set(true);
            for (String addr : op.getAddresses()) {
                persistedAddresses.set(addr, "");
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
        } else {
            throw new RuntimeException("Unkown type!"+input.getClass().getName());
        }
    }
    
    private void makeset(Context context) {
        String address = context.self().id();
        setParent(context, address);
        persistentAddressCount.set(1l);
    }
    
    
    
    private void merge(Context context, MergeOperation merge) {
        final String callerAddress;
        if (context.caller() != null && context.caller().type().equals(TYPE)) {
            callerAddress = context.caller().id();
        } else {
            callerAddress = null;
        }
        String address = context.self().id();
        String parent = persistedParent.get();
        
        //System.out.println(address+" MERGE "+merge.toString()+", parent="+parent+", caller="+callerAddress);
        
        
        if (parent == null) {
            makeset(context);
            for (String from : merge.getFromAddresses()) {
                MergeRootOperation mergeRoot = new MergeRootOperation(address, 1, false);//Crete new objects because sending the same object caused it to be reused in mergeRoot() method
                context.send(new Address(TYPE, from), mergeRoot);
            } 
        } else if (address.equals(parent)) {
            long size = persistentAddressCount.get();
            for (String from : merge.getFromAddresses()) {
                MergeRootOperation mergeRoot = new MergeRootOperation(address, size, false);
                context.send(new Address(TYPE, from), mergeRoot);
            }
        } else {
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
        String parent = persistedParent.get();
        if (parent == null) {
            if (mergeRoot.getRootSize() > 1 || (mergeRoot.getRootSize() == 1 && address.compareTo(mergeRoot.getRoot()) > 0)) {//we use a tiebreaker to avoid creating loops (merge from the lexicographically larger address to the smaller one)
                setParent(context, mergeRoot.getRoot());
                context.send(new Address(TYPE, mergeRoot.getRoot()), new SetSize(1, false));
            } else {
                makeset(context);
                parent = address;
                context.send(new Address(TYPE, mergeRoot.getRoot()), new MergeRootOperation(parent, 1, false));
            }
        } else if (parent.equals(mergeRoot.getRoot())) {//already in the same set
            if (mergeRoot.isCalledFromChild()) {
                context.send(new Address(TYPE, context.caller().id()), new Compress(parent));
            }
        } else if (address.equals(parent)) {
            long size = persistentAddressCount.get();
            if (mergeRoot.getRootSize() > size || (mergeRoot.getRootSize() == size && address.compareTo(mergeRoot.getRoot()) > 0)) {
                setParent(context, mergeRoot.getRoot());
                persistentAddressCount.clear();
                context.send(new Address(TYPE, mergeRoot.getRoot()), new SetSize(size, false));
                
                //long addressCount = persistedClusterAddressCount.getOrDefault(0l);
                boolean hasClusterAddresses = this.hasClusterAddresses.getOrDefault(false);
                if (hasClusterAddresses) {
                    Iterable<String> addresses = this.persistedAddresses.keys();
                    for (String a : addresses) {
                        context.send(new Address(TYPE, mergeRoot.getRoot()), new AddAddressOperation(a));
                    }
                    deleteAddresses(context);
                }
                long txCount = persistedClusterTransactionCount.getOrDefault(0l);
                if (txCount > 0) {
                    long transactionsDeleted = 0;
                    Iterable<Map.Entry<TxPointer, Long>> entries = persistedTransactions.entries();
                    for (Map.Entry<TxPointer, Long> e : entries) {
                        transactionsDeleted++;
                        AddTransactionOperation addTx = new AddTransactionOperation(e.getKey().getTime(), e.getKey().getHeight(), e.getKey().getTx_n(), e.getValue());
                        context.send(new Address(TYPE, mergeRoot.getRoot()), addTx);
                    }
                    if (transactionsDeleted > 0) deleteTransactions(context);
                }
                
            } else {
                context.send(new Address(TYPE, mergeRoot.getRoot()), new MergeRootOperation(address, size, false));//Merge from mergeRoot.getRoot() to this address
            }
        } else {
            if (mergeRoot.isCalledFromChild()) {
                context.send(new Address(TYPE, context.caller().id()), new Compress(parent));
            }
            mergeRoot.setCalledFromChild(true);
            context.send(new Address(TYPE, parent), mergeRoot);
        }
    }
    
    
    private void compress(Context context, Compress compress) {
        context.caller();
        String address = context.self().id();
        String parent = persistedParent.get();
        if (parent == null) {
            throw new RuntimeException("Compress reached root1. This should never happen!");
        } else if (address.equals(parent)) {
            System.out.println(address+"@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
            throw new RuntimeException("Compress reached root2. This should never happen!");
        } else if (parent.equals(compress.getTo())) {
        } else if (parent.equals(context.caller().id())) {
            setParent(context, compress.getTo());
        } else {//already compressed to a node closer to root
        }
    }
    
    private void setSize(Context context, SetSize setSize) {
        String address = context.self().id();
        String parent = persistedParent.get();
        if (address.equals(parent)) {
            persistentAddressCount.updateAndGet(oldSize -> oldSize + setSize.getAdd());
        } else {
            if (setSize.isCalledFromChild()) {
                context.send(new Address(TYPE, context.caller().id()), new Compress(parent));
            }
            setSize.setCalledFromChild(true);
            context.send(new Address(TYPE, parent), setSize);
        }
    }
    
    private void executeCassandraQuery(Context context, String query) {
        throw new RuntimeException("Unsupported operation");
    }
    
    public void addTransaction(Context context, AddTransactionOperation addTransactionOperation) {
        String address = context.self().id();
        String parent = persistedParent.get();
        
        boolean makeSet = false;
        if (parent == null) {
            makeSet = true;
            makeset(context);
            parent = address;
        }
        if (!parent.equals(address)) {
            context.send(new Address(TYPE, parent), addTransactionOperation);
        } else {
            long oldTxCount = makeSet ? 0 : persistedClusterTransactionCount.getOrDefault(0l);
            TxPointer key = new TxPointer(addTransactionOperation.getTime(), addTransactionOperation.getHeight(), addTransactionOperation.getTx_n());
            Long oldDelta = (oldTxCount == 0) ? null : persistedTransactions.get(key);
            long newDelta = oldDelta == null ? addTransactionOperation.getDelta() : addTransactionOperation.getDelta()+oldDelta;
            addTransactionOperation.setDelta(newDelta);
            persistedTransactions.set(key, newDelta);
            if (oldDelta == null) persistedClusterTransactionCount.set(oldTxCount+1);
            sendToCassandra(context, addTransactionOperation);
        }
    }
    
    public void setParent(Context context, String parent) {
        persistedParent.set(parent);
        String address = context.self().id();
        if (address.equals(parent)) {
            sendToCassandra(context, new SetParent(null));
        } else {
            sendToCassandra(context, new SetParent(parent));
        }
    }
    
    public void addAddress(Context context, AddAddressOperation addAddressOp) {
        String address = context.self().id();
        String parent = persistedParent.get();
        boolean makeSet = false;
        if (parent == null) {
            makeSet = true;
            makeset(context);
            parent = address;
        }
        if (!parent.equals(address)) {
            context.send(new Address(TYPE, ""+parent), addAddressOp);
        } else {
            persistedAddresses.set(addAddressOp.getAddress(), "");
            hasClusterAddresses.set(true);
            sendToCassandra(context, addAddressOp);
        }
    }
    
    public void deleteAddresses(Context context) {
        persistedAddresses.clear();
        hasClusterAddresses.clear();
        //persistedClusterAddressCount.clear();
        sendToCassandra(context, new DeleteAddresses());
    }
    
    public void deleteTransactions(Context context) {
        persistedTransactions.clear();
        persistedClusterTransactionCount.clear();
        sendToCassandra(context, new DeleteTransactions());
    }




}
