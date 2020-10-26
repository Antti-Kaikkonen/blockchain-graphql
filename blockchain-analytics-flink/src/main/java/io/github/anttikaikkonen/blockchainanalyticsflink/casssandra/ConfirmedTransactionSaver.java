package io.github.anttikaikkonen.blockchainanalyticsflink.casssandra;


import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.ConfirmedTransaction;
import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.Transaction;

public class ConfirmedTransactionSaver extends CassandraSaverFunction<Transaction> {

    Mapper<Transaction> txMapper;
    Mapper<ConfirmedTransaction> ctMapper;
    
    public ConfirmedTransactionSaver(CassandraSessionBuilder sessionBuilder) {
        super(sessionBuilder);
    }
    
    @Override
    public ListenableFuture saveAsync(Transaction tx) {
        ListenableFuture<Void> txFuture = txMapper.saveAsync(tx);
        ConfirmedTransaction ct = new ConfirmedTransaction(tx.getHeight(), tx.getTxN(), tx.getTxid());
        ListenableFuture<Void> ctFuture = this.ctMapper.saveAsync(ct);
        return Futures.allAsList(txFuture, ctFuture);
    }

    @Override
    public void initMappers(MappingManager manager) {
        this.ctMapper = manager.mapper(ConfirmedTransaction.class);
        this.txMapper = manager.mapper(Transaction.class);
    }
    
}
