package io.github.anttikaikkonen.blockchainanalyticsflink.models;

import java.util.Date;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class ConfirmedTransactionWithInputs extends ConfirmedTransaction {
    
    long timestamp;
    
    TransactionInputWithOutput[] vin;
    
    public ConfirmedTransactionWithInputs(ConfirmedTransaction confirmedTransaction, TransactionInputWithOutput[] vin, long timestamp) {
        this.height = confirmedTransaction.height;
        this.txN = confirmedTransaction.txN;
        this.setTxid(confirmedTransaction.getTxid());
        this.setSize(confirmedTransaction.getSize());
        this.setVersion(confirmedTransaction.getVersion());
        this.setLocktime(confirmedTransaction.getLocktime());
        this.vin = vin;
        //this.setVin(vin);
        this.setVout(confirmedTransaction.getVout());
        this.timestamp = timestamp;
    }
    
    @Override
    public TransactionInputWithOutput[] getVin() {
        return vin;
    }

}