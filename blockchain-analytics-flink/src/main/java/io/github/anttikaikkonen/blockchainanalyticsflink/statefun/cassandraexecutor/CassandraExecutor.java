package io.github.anttikaikkonen.blockchainanalyticsflink.statefun.cassandraexecutor;


import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.CassandraSessionBuilder;
import io.github.anttikaikkonen.blockchainanalyticsflink.statefun.unionfind.AddAddressOperation;
import io.github.anttikaikkonen.blockchainanalyticsflink.statefun.unionfind.AddTransactionOperation;
import java.time.Instant;
import java.util.Date;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.function.Supplier;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.statefun.sdk.AsyncOperationResult;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.StatefulFunction;


public class CassandraExecutor implements StatefulFunction, CheckpointListener {

    public static final FunctionType TYPE = new FunctionType("example", "cassandra-executor");

    private final Session session;
    
    private final Semaphore cassandraLimiter;
    
    PreparedStatement addTransactionStatement;
    
    PreparedStatement setParentStatement;
    
    PreparedStatement makesetStatement;
    
    PreparedStatement addAddressStatement;
    
    PreparedStatement deleteAddressesStatement;
    
    PreparedStatement deleteTransactionsStatement;
    
    
    public CassandraExecutor(CassandraSessionBuilder sessionBuilder, int concurrency) {
        this.session = sessionBuilder.build();
        
        this.cassandraLimiter = new Semaphore(concurrency, true);
        this.addTransactionStatement = this.session.prepare("INSERT INTO cluster_transaction (cluster_id, timestamp, height, tx_n, balance_change) VALUES (?, ?, ?, ?, ?)");
        this.setParentStatement = this.session.prepare("INSERT INTO union_find (address, parent) VALUES (?, ?)");
        this.makesetStatement = this.session.prepare("INSERT INTO union_find (address) VALUES (?)");
        this.addAddressStatement = this.session.prepare("INSERT INTO cluster_address (cluster_id, address) VALUES (?, ?)");
        
        this.deleteAddressesStatement = this.session.prepare("DELETE FROM cluster_address WHERE cluster_id = ?");
        this.deleteTransactionsStatement = this.session.prepare("DELETE FROM cluster_transaction WHERE cluster_id = ?");
        
    }
    

    
    
    @Override
    public void invoke(Context context, Object input) {
        BoundStatement bind = null;
        String metadata = "";
        if (input instanceof AddTransactionOperation) {
            AddTransactionOperation addTransaction = (AddTransactionOperation) input;
            metadata = "1";
            bind = this.addTransactionStatement.bind(context.self().id(), Date.from(Instant.ofEpochMilli(addTransaction.getTime())), addTransaction.getHeight(), addTransaction.getTx_n(), addTransaction.getDelta());
        } else if (input instanceof AddAddressOperation) {
            AddAddressOperation addAddress = (AddAddressOperation) input;
            bind = this.addAddressStatement.bind(context.self().id(), addAddress.getAddress());
            metadata = "2";
        } else if (input instanceof SetParent) {
            SetParent setParent = (SetParent) input;
            if (setParent.getParent() == null) {
                bind = this.makesetStatement.bind(context.self().id());
                metadata = "3";
            } else {
                bind = this.setParentStatement.bind(context.self().id(), setParent.getParent());
                metadata = "4";
            }
            //1 setParentStatement
        } else if (input instanceof DeleteAddresses) {
            bind = this.deleteAddressesStatement.bind(context.self().id());
            metadata = "5";
        } else if (input instanceof DeleteTransactions) {
            bind = this.deleteTransactionsStatement.bind(context.self().id());
            metadata = "6";
        } else if (input instanceof String) {
            throw new RuntimeException("No longer accepting strings");
            /*String query = (String) input;
            try {
                cassandraLimiter.acquire();
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(ex);
            }
            executeCassandraQuery(context, query);*/
        } else if (input instanceof AsyncOperationResult) {
            AsyncOperationResult asyncResult = (AsyncOperationResult) input;
            if (asyncResult.failure()) {
                System.out.println("CASSANDRA FAIl METADATA"+asyncResult.metadata());
                throw new RuntimeException(asyncResult.throwable());
            } else if (asyncResult.successful()) {

            } else if (asyncResult.unknown()) {
                System.out.println("CASSANDRA UNKOWN");
                if (asyncResult.throwable() != null) {
                    throw new RuntimeException(asyncResult.throwable());
                } else {
                    throw new RuntimeException("Unkown cassandra async result");
                }
            }
        } else {
            throw new RuntimeException("CassandraExecutor unkown message type");
        }
        
        if (bind != null) {
             try {
                cassandraLimiter.acquire();
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(ex);
            }
            this.executeCassandraQuery(context, bind, metadata);
        }
        
        
    }
    
    private void executeCassandraQuery(Context context, BoundStatement boundStatement, String metadata) {
        ResultSetFuture resultFuture = this.session.executeAsync(boundStatement);
        CompletableFuture<Void> res = CompletableFuture.supplyAsync(new Supplier<Void>() {
            @Override
            public Void get() {
                resultFuture.getUninterruptibly();
                return null;
                //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }
        });
        //resultFuture.
        //CompletionStage<AsyncResultSet> executeAsync = this.session.executeAsync(boundStatement);
        //BoundStatement bind = this.session.prepare("").bind(null);
        context.registerAsyncOperation(metadata, res);
        res.thenRunAsync(() -> {
            cassandraLimiter.release();
        });
    }

    @Override
    public void notifyCheckpointComplete(long arg0) throws Exception {
        System.out.println("CASSANDRA EXECUTOR CHECKPOINT COMPLETED!!!!!!!!");
    }
    
 
}
