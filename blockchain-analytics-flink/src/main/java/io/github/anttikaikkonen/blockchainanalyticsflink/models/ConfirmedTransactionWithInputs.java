package io.github.anttikaikkonen.blockchainanalyticsflink.models;

import java.util.Date;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Data
@NoArgsConstructor
public class ConfirmedTransactionWithInputs extends ConfirmedTransaction {
    
    private long timestamp;
    
    private TransactionInputWithOutput[] inputsWithOutputs;
    //private TransactionInputWithOutput[] vin;
    
    public ConfirmedTransactionWithInputs(ConfirmedTransaction confirmedTransaction, TransactionInputWithOutput[] vin, long timestamp) {
        this.setHeight(confirmedTransaction.getHeight());
        this.setTxN(confirmedTransaction.getTxN());
        this.setTxid(confirmedTransaction.getTxid());
        this.setSize(confirmedTransaction.getSize());
        this.setVersion(confirmedTransaction.getVersion());
        this.setLocktime(confirmedTransaction.getLocktime());
        this.setVin(vin);
        this.inputsWithOutputs = vin;
        //this.setVin(vin);
        this.setVout(confirmedTransaction.getVout());
        this.timestamp = timestamp;
    }
    
   
    /*@Override
    public TransactionInputWithOutput[] getVin() {
        return vin;
    }*/

}