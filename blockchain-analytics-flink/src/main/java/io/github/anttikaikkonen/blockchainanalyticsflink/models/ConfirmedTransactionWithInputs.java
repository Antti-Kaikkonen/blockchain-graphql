package io.github.anttikaikkonen.blockchainanalyticsflink.models;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class ConfirmedTransactionWithInputs extends ConfirmedTransaction {
    
    TransactionInputWithOutput[] vin;
    
    public ConfirmedTransactionWithInputs(ConfirmedTransaction confirmedTransaction, TransactionInputWithOutput[] vin) {
        this.height = confirmedTransaction.height;
        this.txN = confirmedTransaction.txN;
        this.setTxid(confirmedTransaction.getTxid());
        this.setSize(confirmedTransaction.getSize());
        this.setVersion(confirmedTransaction.getVersion());
        this.setLocktime(confirmedTransaction.getLocktime());
        this.vin = vin;
        //this.setVin(vin);
        this.setVout(confirmedTransaction.getVout());
    }
    
    @Override
    public TransactionInputWithOutput[] getVin() {
        return vin;
    }

}