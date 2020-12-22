package io.github.anttikaikkonen.blockchainanalyticsflink.models;

import io.github.anttikaikkonen.bitcoinrpcclientjava.models.Transaction;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@NoArgsConstructor
public class ConfirmedTransaction extends Transaction {
    
    private int height;
    private int txN;

    public ConfirmedTransaction(Transaction tx, int height, int txN) {
        this.height = height;
        this.txN = txN;
        setLocktime(tx.getLocktime());
        setSize(tx.getSize());
        setTxid(tx.getTxid());
        setVersion(tx.getVersion());
        setVin(tx.getVin());
        setVout(tx.getVout());
    }

}