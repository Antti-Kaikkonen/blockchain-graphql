package io.github.anttikaikkonen.blockchainanalyticsflink.models;

import io.github.anttikaikkonen.bitcoinrpcclientjava.models.TransactionOutput;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
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
        this.setVout(confirmedTransaction.getVout());
        this.timestamp = timestamp;
    }
    
    public boolean possiblyCoinJoin() {
        Set<String> inputAddresses = new HashSet<>();
        for (TransactionInputWithOutput vin : getInputsWithOutputs()) {
            try {
                String address = vin.getSpentOutput().getScriptPubKey().getAddresses()[0];
                if (address != null) {
                    inputAddresses.add(address);
                }
            } catch(Exception ex) {
            }
        }
        if (inputAddresses.size() < 2) return false;
        Map<Long, String> outputAmount2Address = new HashMap<>();
        for (TransactionOutput vout : getVout()) {
            String address;
            try {
                address = vout.getScriptPubKey().getAddresses()[0];
                if (address == null) continue;
            } catch(Exception ex) {
                continue;
            }
            long value = Math.round(vout.getValue()*1e8);
            String equalAmountAddress = outputAmount2Address.get(value);
            if (equalAmountAddress != null && !equalAmountAddress.equals(address)) {
                return true;
            } else {
                outputAmount2Address.put(value, address);
            }
        }
        return false;
    }
   

}