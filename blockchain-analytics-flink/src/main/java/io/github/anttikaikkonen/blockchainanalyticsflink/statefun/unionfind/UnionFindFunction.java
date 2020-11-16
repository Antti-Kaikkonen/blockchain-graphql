package io.github.anttikaikkonen.blockchainanalyticsflink.statefun.unionfind;


import io.github.anttikaikkonen.blockchainanalyticsflink.statefun.cassandraexecutor.AddressOperation;
import io.github.anttikaikkonen.blockchainanalyticsflink.statefun.cassandraexecutor.SetParent;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.annotations.Persisted;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.state.PersistedStateRegistry;
import org.apache.flink.statefun.sdk.state.PersistedTable;
import org.apache.flink.statefun.sdk.state.PersistedValue;
import io.github.anttikaikkonen.blockchainanalyticsflink.statefun.cassandraexecutor.DeleteTransactions;
import io.github.anttikaikkonen.blockchainanalyticsflink.statefun.cassandraexecutor.DeleteAddresses;

public class UnionFindFunction implements StatefulFunction {
    
    public static final EgressIdentifier<AddressOperation> EGRESS = new EgressIdentifier<>("address_clustering", "union_find_sink", AddressOperation.class);

    
    public static final FunctionType TYPE = new FunctionType("address_clustering", "union_find");
    
    @Persisted
    private final PersistedValue<String> persistedParent = PersistedValue.of("parent", String.class);
    
    @Persisted
    private final PersistedValue<Long> persistentAddressCount = PersistedValue.of("addressCount", Long.class);
    
    /*@Persisted
    private final PersistedStateRegistry registry = new PersistedStateRegistry();*/
    
    @Persisted
    private final PersistedTable<String, String> persistedAddresses = PersistedTable.of("addresses", String.class, String.class);

    @Persisted
    private final PersistedTable<TxPointer, Long> persistedTransactions = PersistedTable.of("transactions", TxPointer.class, Long.class);
    
    /*@Persisted
    private final PersistedValue<Integer> persistedTransactionCount = PersistedValue.of("transactionCount", Integer.class);*/
    
    /*@Persisted
    private final PersistedTable<TxPointer, Long> persistedTransactions = PersistedTable.of("transactions", TxPointer.class, Long.class);*/
    
    /*@Persisted
    private final PersistedAppendingBuffer<String> persistedAddresses = PersistedAppendingBuffer.of("addresses", String.class);*/
    
    
    
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
            context.send(new Address(TYPE, parent), op);
        } else {
            /*if (this.persistedAddresses == null) {
                this.persistedAddresses = PersistedTable.of("addresses", String.class, String.class);
                this.registry.registerTable(persistedAddresses);

                this.persistedTransactions = PersistedTable.of("transactions", TxPointer.class, Long.class);
                this.registry.registerTable(persistedTransactions);
                
                for (BlockTx tx : op.getBlockTxs()) {
                    TxPointer key = new TxPointer(op.getTimestamp(), op.getHeight(), tx.getTxN());
                    persistedTransactions.set(key, tx.getDelta());
                    sendToCassandra(context, new AddTransactionOperation(op.getTimestamp(), op.getHeight(), tx.getTxN(), tx.getDelta()));
                }
            } else {*/
                for (BlockTx tx : op.getBlockTxs()) {
                    TxPointer key = new TxPointer(op.getTimestamp(), op.getHeight(), tx.getTxN());
                    Long oldDelta = makeSet ? null : persistedTransactions.get(key);
                    //Long oldDelta = persistedTransactions.get(key);
                    long newDelta = oldDelta == null ? tx.getDelta() : tx.getDelta()+oldDelta;
                    persistedTransactions.set(key, newDelta);
                    sendToCassandra(context, new AddTransactionOperation(op.getTimestamp(), op.getHeight(), tx.getTxN(), newDelta));
                }
            //}
            for (String addr : op.getAddresses()) {
                this.persistedAddresses.set(addr, "");
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
            compress2(context, e);
        } else if (input instanceof SetSize) {
            SetSize e = (SetSize) input;
            setSize(context, e);
        } else if (input instanceof AddTransactionOperation[]) {
            AddTransactionOperation[] e = (AddTransactionOperation[]) input;
            addTransactions(context, e);
        } else if (input instanceof AddTransactionOperation) {
            AddTransactionOperation e = (AddTransactionOperation) input;
            addTransaction(context, e);
        } else if (input instanceof AddAddressOperation[]) {
            AddAddressOperation[] e = (AddAddressOperation[]) input;
            addAddresses(context, e);
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
        //sendToCassandra(context, new AddAddressOperation(address));TODO
        
        //executeCassandraQuery(context, "INSERT INTO dash.cluster_address (cluster_id, address) VALUES ('"+address+"', '"+address+"')");
    }
    
    private void merge(Context context, MergeOperation merge) {
        if (merge.getVisited().size() > 10) {
            System.out.println("Merge Visited.size = "+merge.getVisited().size());
        }
        String address = context.self().id();
        
        String parent = persistedParent.get();
        if (parent == null) {
            makeset(context);
            MergeRootOperation mergeRoot = new MergeRootOperation(address, 1, merge.getVisited(), new ArrayList<>()/*, merge.getTransaction()*/);
            context.send(new Address(TYPE, merge.getTo()), mergeRoot);
        } else if (parent.equals(merge.getTo())) {
            compressTo(context, parent,  merge.getVisited());
            //if (merge.getTransaction() != null) context.send(new Address(TYPE, parent), merge.getTransaction());
            //TODO save tx, path compression
        } else if (address.equals(parent)) {
            MergeRootOperation mergeRoot = new MergeRootOperation(address, persistentAddressCount.get(), merge.getVisited(), new ArrayList<>()/*, merge.getTransaction()*/);
            context.send(new Address(TYPE, merge.getTo()), mergeRoot);
        } else {
            merge.getVisited().add(address);
            context.send(new Address(TYPE, parent), merge);
        }
    }
    
    private void compressTo(Context context, String to, Collection<String> addresses) {
        
        Compress compress = new Compress(to);
        for (String a: addresses) {
            context.send(new Address(TYPE, a), compress);
        }
    }
    
    private void mergeRoot(Context context, MergeRootOperation mergeRoot) {
        if (mergeRoot.getVisited().size() > 10) {
            System.out.println("Mergeroot Visited.size = "+mergeRoot.getVisited().size());
        }
        String address = context.self().id();
        String parent = persistedParent.get();
        if (parent == null) {
            if (mergeRoot.getRootSize() > 1 || (mergeRoot.getRootSize() == 1 && address.compareTo(mergeRoot.getRoot()) < 0)) {//we use a tiebreaker to avoid creating loops
                setParent(context, mergeRoot.getRoot());
                context.send(new Address(TYPE, mergeRoot.getRoot()), new SetSize(1));
                //if (mergeRoot.getTransaction() != null) context.send(new Address(TYPE, mergeRoot.getRoot())/*, mergeRoot.getTransaction()*/);
                Set<String> compresAddresses = new HashSet<>(mergeRoot.getVisited());
                for (int i = 0; i < mergeRoot.getRootVisited().size()-1; i++) {
                    compresAddresses.add(mergeRoot.getRootVisited().get(i));
                }
                compressTo(context, mergeRoot.getRoot(), compresAddresses);
                //context.send(new Address(TYPE, mergeRoot.getRoot()), new AddAddressOperation(address));
            } else {
                makeset(context);
                parent = address;
                context.send(new Address(TYPE, mergeRoot.getRoot()), new MergeRootOperation(parent, 1, mergeRoot.getVisited(), mergeRoot.getRootVisited()/*, mergeRoot.getTransaction()*/));

            }
        } else if (parent.equals(mergeRoot.getRoot())) {//already in the same set
            //if (mergeRoot.getTransaction() != null) {
            //    addTransaction(context, mergeRoot.getTransaction());
            //}
            Set<String> compresAddresses = new HashSet<>(mergeRoot.getVisited());
            for (int i = 0; i < mergeRoot.getRootVisited().size()-1; i++) {
                compresAddresses.add(mergeRoot.getRootVisited().get(i));
            }
            compressTo(context, parent, compresAddresses);
        } else if (address.equals(parent)) {
            long size = persistentAddressCount.get();
            if (mergeRoot.getRootSize() > size || (mergeRoot.getRootSize() == size && address.compareTo(mergeRoot.getRoot()) < 0)) {
                setParent(context, mergeRoot.getRoot());
                persistentAddressCount.clear();

                Set<String> compresAddresses = new HashSet<>(mergeRoot.getVisited());
                compresAddresses.addAll(mergeRoot.getRootVisited());
                compressTo(context, mergeRoot.getRoot(), compresAddresses);
                
                context.send(new Address(TYPE, mergeRoot.getRoot()), new SetSize(size));
                
                //if (persistedAddresses != null) {
                    ArrayList<AddAddressOperation> addAddressOperations = new ArrayList<>(100);
                    //addAddressOperations.add(new AddAddressOperation(address));//TODO remove?
                    Iterable<String> addresses = this.persistedAddresses.keys();
                    for (String a : addresses) {
                        addAddressOperations.add(new AddAddressOperation(a));
                        if (addAddressOperations.size() == 100) {
                            context.send(new Address(TYPE, mergeRoot.getRoot()), addAddressOperations.toArray(new AddAddressOperation[0]));
                            addAddressOperations.clear();
                        }
                    }
                    if (!addAddressOperations.isEmpty()) context.send(new Address(TYPE, mergeRoot.getRoot()), addAddressOperations.toArray(new AddAddressOperation[0]));
                    deleteAddresses(context);
                //}
                //if (persistedTransactions != null) {
                    ArrayList<AddTransactionOperation> addTransactionOperations = new ArrayList<>(100);
                    Iterable<Map.Entry<TxPointer, Long>> entries = persistedTransactions.entries();
                    for (Map.Entry<TxPointer, Long> e : entries) {
                        AddTransactionOperation addTx = new AddTransactionOperation(e.getKey().getTime(), e.getKey().getHeight(), e.getKey().getTx_n(), e.getValue());
                        addTransactionOperations.add(addTx);
                        if (addTransactionOperations.size() == 100) {
                            context.send(new Address(TYPE, ""+mergeRoot.getRoot()), addTransactionOperations.toArray(new AddTransactionOperation[0]));
                            addTransactionOperations.clear();
                        }
                    }
                    if (!addTransactionOperations.isEmpty()) context.send(new Address(TYPE, ""+parent), addTransactionOperations.toArray(new AddTransactionOperation[0]));
                    deleteTransactions(context);
                //}
                
            } else {
                context.send(new Address(TYPE, mergeRoot.getRoot()), new MergeRootOperation(parent, size, mergeRoot.getVisited(), mergeRoot.getRootVisited()/*, mergeRoot.getTransaction()*/));
            }
        } else {
            mergeRoot.getVisited().add(address);
            context.send(new Address(TYPE, parent), mergeRoot);
        }
    }
    
    private void compress2(Context context, Compress compress2) {
        setParent(context, compress2.getParent());
    }
    
    private void setSize(Context context, SetSize setSize) {
        String address = context.self().id();
        String parent = persistedParent.get();
        if (address.equals(parent)) {
            persistentAddressCount.updateAndGet(oldSize -> oldSize + setSize.getAdd());
        } else {
            context.send(new Address(TYPE, parent), setSize);
        }
    }
    
    private void executeCassandraQuery(Context context, String query) {
        throw new RuntimeException("Unsupported operation");
        /*if (true) {
            context.sendAfter(Duration.ofMinutes(4), new Address(CassandraExecutor.TYPE, ""+context.self().id()), query);
            return;
        }*/
    }
    
    public void addTransactions(Context context, AddTransactionOperation[] addTransactionsOperations) {
        String address = context.self().id();
        String parent = persistedParent.get();
        
        boolean makeSet = false;
        if (parent == null) {//Makeset
            makeSet = true;
            makeset(context);
            parent = address;
        }
        if (!parent.equals(address)) {
            context.send(new Address(TYPE, parent), addTransactionsOperations);
        } else {
            /*if (this.persistedTransactions == null) {
                System.out.println("CREATE 1 @@@");
                this.persistedTransactions = PersistedTable.of("transactions", TxPointer.class, Long.class);
                this.registry.registerTable(this.persistedTransactions);
            }*/
            
            //int new_tx_count = 0;
            for (AddTransactionOperation addTransactionOperation : addTransactionsOperations) {
                TxPointer key = new TxPointer(addTransactionOperation.getTime(), addTransactionOperation.getHeight(), addTransactionOperation.getTx_n());
                Long oldDelta = makeSet ? null : persistedTransactions.get(key);
                //Long oldDelta = null;//TODO!!!
                long newDelta = oldDelta == null ? addTransactionOperation.getDelta() : addTransactionOperation.getDelta()+oldDelta;
                addTransactionOperation.setDelta(newDelta);
                persistedTransactions.set(key, newDelta);
                //executeCassandraQuery(context, "INSERT INTO dash.cluster_transaction (cluster_id, timestamp, height, tx_n, balance_change) VALUES ('"+address+"', "+key.getTime()+", "+key.getHeight()+", "+key.getTx_n()+", "+newDelta+")");
                //sendToCassandra(context, addTransactionOperation);
                //if (oldDelta == null) {
                //    new_tx_count++;
                //}
            }
            sendToCassandra(context, addTransactionsOperations);
            //final int new_tx_count_final = new_tx_count;
            //persistedTransactionCount.updateAndGet(oldCount -> oldCount == null ? new_tx_count_final: oldCount + new_tx_count_final);
        }
    }
    
    public void addTransaction(Context context, AddTransactionOperation addTransactionOperation) {
        addTransactions(context, new AddTransactionOperation[] {addTransactionOperation});
    }
    
    public void setParent(Context context, String parent) {
        persistedParent.set(parent);
        String address = context.self().id();
        if (address.equals(parent)) {
            sendToCassandra(context, new SetParent(null));
            //executeCassandraQuery(context, "INSERT INTO dash.union_find (address) VALUES ('"+address+"')");
        } else {
            sendToCassandra(context, new SetParent(parent));
            //executeCassandraQuery(context, "INSERT INTO dash.union_find (address, parent) VALUES ('"+address+"', '"+parent+"')");
        }
    }
    
    public void addAddresses(Context context, AddAddressOperation[] addAddressOps) {
        String address = context.self().id();
        String parent = persistedParent.get();
        if (!parent.equals(address)) {
            context.send(new Address(TYPE, ""+parent), addAddressOps);
        } else {
            /*if (persistedAddresses == null) {
                System.out.println("CREATE 2 @@@");
                this.persistedAddresses = PersistedTable.of("addresses", String.class, String.class);
                this.registry.registerTable(persistedAddresses);
            }*/
            for (AddAddressOperation addAddressOp : addAddressOps) {
                persistedAddresses.set(addAddressOp.getAddress(), "");
                //sendToCassandra(context, addAddressOp);
                //executeCassandraQuery(context, "INSERT INTO dash.cluster_address (cluster_id, address) VALUES ('"+address+"', '"+addAddressOp.getAddress()+"')");
            }
            sendToCassandra(context, addAddressOps);
        }
    }
    
    public void addAddress(Context context, AddAddressOperation addAddressOp) {
        addAddresses(context, new AddAddressOperation[] {addAddressOp});
    }
    
    public void deleteAddresses(Context context) {
        persistedAddresses.clear();
        sendToCassandra(context, new DeleteAddresses());
        //executeCassandraQuery(context, "DELETE FROM dash.cluster_address WHERE cluster_id = '"+context.self().id()+"'");
    }
    
    public void deleteTransactions(Context context) {
        persistedTransactions.clear();
        sendToCassandra(context, new DeleteTransactions());
        //executeCassandraQuery(context, "DELETE FROM dash.cluster_transaction WHERE cluster_id = '"+context.self().id()+"'");
    }




}