package io.github.anttikaikkonen.blockchainanalyticsflink.models;

import io.github.anttikaikkonen.bitcoinrpcclientjava.models.Transaction;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;


@Getter
@Setter
@NoArgsConstructor
public class ConfirmedTransaction extends Transaction {
    
    int height;
    int txN;

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