package io.github.anttikaikkonen.blockchainanalyticsflink.statefun.sink;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import io.github.anttikaikkonen.blockchainanalyticsflink.AddressBalanceCandlesticks;
import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.CassandraSessionBuilder;
import io.github.anttikaikkonen.blockchainanalyticsflink.statefun.cassandraexecutor.AddressOperation;
import io.github.anttikaikkonen.blockchainanalyticsflink.statefun.cassandraexecutor.DeleteAddresses;
import io.github.anttikaikkonen.blockchainanalyticsflink.statefun.cassandraexecutor.DeleteTransactions;
import io.github.anttikaikkonen.blockchainanalyticsflink.statefun.cassandraexecutor.SetParent;
import io.github.anttikaikkonen.blockchainanalyticsflink.statefun.unionfind.AddAddressOperation;
import io.github.anttikaikkonen.blockchainanalyticsflink.statefun.unionfind.AddTransactionOperation;
import java.time.Instant;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.runtime.operators.GenericWriteAheadSink;

public class UnionFindSink extends GenericWriteAheadSink<AddressOperation> {

    private final CassandraSessionBuilder sessionBuilder;
    
    private transient Session session;
    
    private transient PreparedStatement addTransactionStatement;
    
    private transient PreparedStatement setParentStatement;
    
    private transient PreparedStatement makesetStatement;
    
    private transient PreparedStatement addAddressStatement;
    
    private transient PreparedStatement deleteAddressesStatement;
    
    private transient PreparedStatement deleteTransactionsStatement;
    
    private Semaphore semaphore;
    
    public UnionFindSink(TypeSerializer<AddressOperation> serializer, CassandraSessionBuilder sessionBuilder) throws Exception {
        super(new ScyllaCommitter(sessionBuilder), serializer, UUID.randomUUID().toString().replace("-", "_"));
        this.sessionBuilder = sessionBuilder;
    }

    @Override
    public void open() throws Exception {
        super.open();
        this.semaphore = new Semaphore(AddressBalanceCandlesticks.CASSANDRA_CONCURRENT_REQUESTS, true);
        this.session = this.sessionBuilder.build();
        this.addTransactionStatement = this.session.prepare("INSERT INTO cluster_transaction (cluster_id, timestamp, height, tx_n, balance_change) VALUES (?, ?, ?, ?, ?)");
        this.setParentStatement = this.session.prepare("INSERT INTO union_find (address, parent) VALUES (?, ?)");
        this.makesetStatement = this.session.prepare("INSERT INTO union_find (address) VALUES (?)");
        this.addAddressStatement = this.session.prepare("INSERT INTO cluster_address (cluster_id, address) VALUES (?, ?)");

        this.deleteAddressesStatement = this.session.prepare("DELETE FROM cluster_address WHERE cluster_id = ?");
        this.deleteTransactionsStatement = this.session.prepare("DELETE FROM cluster_transaction WHERE cluster_id = ?");
    }
    
    @Override
    public void close() throws Exception {
        super.close();
        try {
            if (session != null) {
                session.close();
            }
        } catch (Exception e) {
            LOG.error("Error while closing session.", e);
        }
        try {
            if (session != null && session.getCluster() != null) {
                session.getCluster().close();
            }
        } catch (Exception e) {
            LOG.error("Error while closing cluster.", e);
        }
    }
    
    
    private void execute(BoundStatement bind, long timestamp, FutureCallback<ResultSet> callback) throws InterruptedException {
        bind.setDefaultTimestamp(timestamp);
        semaphore.acquire();
        ResultSetFuture resultFuture = this.session.executeAsync(bind);
        if (resultFuture != null) {
            Futures.addCallback(resultFuture, callback);
        }
    }
    
    @Override
    protected boolean sendValues(Iterable<AddressOperation> iterable, long checkpointId, long timestamp) throws Exception {
        final AtomicInteger updatesCount = new AtomicInteger(0);
        final AtomicInteger updatesConfirmed = new AtomicInteger(0);
        final AtomicReference<Throwable> exception = new AtomicReference<>();
        FutureCallback<ResultSet> callback = new FutureCallback<ResultSet>() {
            @Override
            public void onSuccess(ResultSet resultSet) {
                semaphore.release();
                updatesConfirmed.incrementAndGet();
                if (updatesCount.get() > 0) { // only set if all updates have been sent
                    if (updatesCount.get() == updatesConfirmed.get()) {
                        synchronized (updatesConfirmed) {
                            updatesConfirmed.notifyAll();
                        }
                    }
                }
            }

            @Override
            public void onFailure(Throwable throwable) {
                semaphore.release();
                if (exception.compareAndSet(null, throwable)) {
                    System.out.println("ERROR SENDING VALUE1: "+exception.get());
                    LOG.error("Error while sending value.", throwable);
                    synchronized (updatesConfirmed) {
                        updatesConfirmed.notifyAll();
                    }
                }
            }
        };

        
        int updatesSent = 0;
        
        for (AddressOperation op : iterable) {
            BoundStatement bind;
            Object input = op.getOp();
            String address = op.getAddress();
            if (input instanceof SetParent) {
                SetParent setParent = (SetParent) input;
                if (setParent.getParent() == null) {
                    bind = this.makesetStatement.bind(address);
                } else {
                    bind = this.setParentStatement.bind(address, setParent.getParent());
                }
            } else if (input instanceof DeleteTransactions) {
                bind = this.deleteTransactionsStatement.bind(address);
            } else if (input instanceof DeleteAddresses) {
                bind = this.deleteAddressesStatement.bind(address);
            } else if (input instanceof AddAddressOperation) {
                AddAddressOperation addAddress = (AddAddressOperation) input;
                bind = this.addAddressStatement.bind(address, addAddress.getAddress());
            } else if (input instanceof AddTransactionOperation) {
                AddTransactionOperation addTransaction = (AddTransactionOperation) input;
                bind = this.addTransactionStatement.bind(address, Date.from(Instant.ofEpochMilli(addTransaction.getTime())), addTransaction.getHeight(), addTransaction.getTx_n(), addTransaction.getDelta());
            } else if (input instanceof AddAddressOperation[]) {
                AddAddressOperation[] addAddresses = (AddAddressOperation[]) input;
                for (AddAddressOperation addAddress : addAddresses) {
                    bind = this.addAddressStatement.bind(address, addAddress.getAddress());
                    bind.setDefaultTimestamp(timestamp);
                    semaphore.acquire();
                    ResultSetFuture resultFuture = this.session.executeAsync(bind);
                    updatesSent++;
                    if (resultFuture != null) {
                        Futures.addCallback(resultFuture, callback);
                    }
                } 
                continue;
            } else if (input instanceof AddTransactionOperation[]) {
                AddTransactionOperation[] addTransactions = (AddTransactionOperation[]) input;
                for (AddTransactionOperation addTransaction : addTransactions) {
                    bind = this.addTransactionStatement.bind(address, Date.from(Instant.ofEpochMilli(addTransaction.getTime())), addTransaction.getHeight(), addTransaction.getTx_n(), addTransaction.getDelta());
                    bind.setDefaultTimestamp(timestamp);
                    semaphore.acquire();
                    ResultSetFuture resultFuture = this.session.executeAsync(bind);
                    updatesSent++;
                    if (resultFuture != null) {
                        Futures.addCallback(resultFuture, callback);
                    }
                }
                continue;
            } else {
                throw new RuntimeException("Unsupported type "+input.getClass().toString());
            }
            bind.setDefaultTimestamp(timestamp);
            semaphore.acquire();
            ResultSetFuture resultFuture = this.session.executeAsync(bind);
            updatesSent++;
            if (resultFuture != null) {
                Futures.addCallback(resultFuture, callback);
            }
            
        }
        updatesCount.set(updatesSent);
        
        synchronized (updatesConfirmed) {
            while (exception.get() == null && updatesSent != updatesConfirmed.get()) {
                updatesConfirmed.wait();
            }
        }
        if (exception.get() != null) {
            System.out.println("ERROR SENDING VALUE2: "+exception.get());
            LOG.warn("Sending a value failed.", exception.get());
            return false;
        } else {
            return true;
        }
    }

}
