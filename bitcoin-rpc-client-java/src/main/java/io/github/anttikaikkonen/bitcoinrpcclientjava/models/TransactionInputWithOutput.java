package io.github.anttikaikkonen.bitcoinrpcclientjava.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TransactionInputWithOutput {
    
    public TransactionInputWithOutput(TransactionInput input, TransactionOutput spentOutput) {
        this.coinbase = input.coinbase;
        this.sequence = input.sequence;
        this.txid = input.txid;
        this.vout = input.vout;
        this.scriptSig = input.scriptSig;
        this.spentOutput = spentOutput;
    }
    
    String coinbase;
    long sequence;
    String txid;
    int vout;
    ScriptSig scriptSig;
    TransactionOutput spentOutput;
}
