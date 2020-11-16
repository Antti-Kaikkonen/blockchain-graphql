package io.github.anttikaikkonen.blockchainanalyticsflink.casssandra;

import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.github.anttikaikkonen.bitcoinrpcclientjava.models.TransactionOutput;
import io.github.anttikaikkonen.blockchainanalyticsflink.Main;
import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.AddressTransaction;
import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.ConfirmedTransaction;
import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.ScriptPubKey;
import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.ScriptSig;
import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.Transaction;
import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.TransactionInput;
import io.github.anttikaikkonen.blockchainanalyticsflink.models.ConfirmedTransactionWithInputs;
import io.github.anttikaikkonen.blockchainanalyticsflink.models.TransactionInputWithOutput;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import org.apache.flink.configuration.Configuration;

public class TransactionSink extends CassandraSaverFunction<ConfirmedTransactionWithInputs> {

    private Mapper<Transaction> txMapper;
    private Mapper<ConfirmedTransaction> ctMapper;
    private Mapper<io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.TransactionOutput> voutMapper;
    private Mapper<io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.TransactionInput> vinMapper;
    private Mapper<AddressTransaction> addressTransactionMapper;
    private Semaphore semaphore;
    
    public TransactionSink(CassandraSessionBuilder sessionBuilder) {
        super(sessionBuilder);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.semaphore = new Semaphore(Main.CASSANDRA_CONCURRENT_REQUESTS, false);
    }
    
    @Override
    public ListenableFuture saveAsync(ConfirmedTransactionWithInputs transaction) {
        FutureCallback<Void> releaseSemaphore = new FutureCallback<Void>() {
            @Override
            public void onSuccess(Void resultSet) {
                semaphore.release();
            }

            @Override
            public void onFailure(Throwable throwable) {
                semaphore.release();
            }
        };
        
        ArrayList<ListenableFuture<Void>> futures = new ArrayList<>();
        
        List<io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.TransactionInput> inputs = new ArrayList<>();
        
        io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.Transaction tx = new io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.Transaction();
        tx.setHeight(transaction.getHeight());
        tx.setLocktime(transaction.getLocktime());
        tx.setSize(transaction.getSize());
        tx.setTxN(transaction.getTxN());
        tx.setTxid(transaction.getTxid());
        tx.setVersion(transaction.getVersion());
        tx.setOutput_count(transaction.getVout().length);
        tx.setInput_count(transaction.getVin().length);
        long fee = 0;
        for (TransactionInputWithOutput vin : transaction.getVin()) {

            if (vin.getTxid() != null) {
                io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.TransactionOutput vout = new io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.TransactionOutput();
                vout.setTxid(vin.getTxid());
                vout.setN(vin.getVout());
                vout.setSpending_txid(transaction.getTxid());
                vout.setSpending_index(transaction.getTxN());
                this.semaphore.acquireUninterruptibly();
                ListenableFuture<Void> future = voutMapper.saveAsync(vout, Mapper.Option.saveNullFields(false));//Update query
                Futures.addCallback(future, releaseSemaphore);
                fee += Math.round(vin.getSpentOutput().getValue()*1e8);
            }

            io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.TransactionInput vin2 = new TransactionInput();
            vin2.setSpending_txid(transaction.getTxid());
            vin2.setSpending_index(transaction.getTxN());
            vin2.setCoinbase(vin.getCoinbase());
            vin2.setCoinbase(vin.getCoinbase());
            if (vin.getScriptSig() != null) {
                ScriptSig scriptSig = new ScriptSig(vin.getScriptSig());
                vin2.setScriptSig(scriptSig);
            }
            vin2.setSequence(vin.getSequence());
            vin2.setTxid(vin.getTxid());
            vin2.setVout(vin.getVout());
            this.semaphore.acquireUninterruptibly();
            ListenableFuture<Void> future = vinMapper.saveAsync(vin2);
            Futures.addCallback(future, releaseSemaphore);
            futures.add(future);
        }
        for (TransactionOutput vout : transaction.getVout()) {
            io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.TransactionOutput vout2 = new io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.TransactionOutput();
            vout2.setTxid(transaction.getTxid());
            vout2.setN(transaction.getTxN());
            ScriptPubKey scriptpubKey = new ScriptPubKey(vout.getScriptPubKey());
            vout2.setScriptPubKey(scriptpubKey);
            vout2.setValue(vout.getValue());
            this.semaphore.acquireUninterruptibly();
            ListenableFuture<Void> future = voutMapper.saveAsync(vout2);
            Futures.addCallback(future, releaseSemaphore);
            futures.add(future);

            fee -= Math.round(vout.getValue()*1e8);
        }
        if (transaction.getTxN() == 0) {
            tx.setTx_fee(0);
        } else {
            tx.setTx_fee(fee);
        }
        this.semaphore.acquireUninterruptibly();
        ListenableFuture<Void> future = txMapper.saveAsync(tx);
        Futures.addCallback(future, releaseSemaphore);
        futures.add(future);
        ConfirmedTransaction ct = new ConfirmedTransaction(tx.getHeight(), tx.getTxN(), tx.getTxid());
        this.semaphore.acquireUninterruptibly();
        future = ctMapper.saveAsync(ct);
        Futures.addCallback(future, releaseSemaphore);
        futures.add(future);
        
        
        Map<String, Long> addressDeltas = new HashMap<>();
        for (TransactionOutput vout : transaction.getVout()) {
            if (vout.getScriptPubKey().getAddresses() == null) continue;
            if (vout.getScriptPubKey().getAddresses().length != 1) continue;
            String address = vout.getScriptPubKey().getAddresses()[0];
            long value = Math.round(vout.getValue()*1e8);
            addressDeltas.compute(address, (key, oldDelta) -> oldDelta == null ? value : oldDelta + value);
        }
        for (TransactionInputWithOutput vin : transaction.getVin()) {
            if (vin.getSpentOutput() == null) continue;
            if (vin.getSpentOutput().getScriptPubKey().getAddresses() == null) continue;
            if (vin.getSpentOutput().getScriptPubKey().getAddresses().length != 1) continue;
            String address = vin.getSpentOutput().getScriptPubKey().getAddresses()[0];
            long value = Math.round(vin.getSpentOutput().getValue()*1e8);
            addressDeltas.compute(address, (key, oldDelta) -> oldDelta == null ? -value : oldDelta - value);
        }
        for (String address : addressDeltas.keySet()) {
            long delta = addressDeltas.get(address);
            AddressTransaction addressTransaction = new AddressTransaction(address, null, transaction.getHeight(), transaction.getTxN(), delta);
            addressTransaction.setTimestamp(Date.from(Instant.ofEpochMilli(transaction.getTimestamp())));
            this.semaphore.acquireUninterruptibly();
            future = addressTransactionMapper.saveAsync(addressTransaction);
            Futures.addCallback(future, releaseSemaphore);
            futures.add(future);
        }
        
        
        return Futures.allAsList(futures);
    }
    

    @Override
    public void initMappers(MappingManager manager) {
        this.ctMapper = manager.mapper(ConfirmedTransaction.class);
        this.txMapper = manager.mapper(Transaction.class);
        this.voutMapper = manager.mapper(io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.TransactionOutput.class);
        this.vinMapper = manager.mapper(io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.TransactionInput.class);
        this.addressTransactionMapper = manager.mapper(AddressTransaction.class);
    }

}
