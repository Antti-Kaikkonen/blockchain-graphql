package io.github.anttikaikkonen.bitcoinrpcclientjava.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TransactionInputWithOutput {

    public TransactionInputWithOutput(TransactionInput input, TransactionOutput spentOutput) {
        this.coinbase = input.getCoinbase();
        this.sequence = input.getSequence();
        this.txid = input.getTxid();
        this.vout = input.getVout();
        this.scriptSig = input.getScriptSig();
        this.spentOutput = spentOutput;
    }

    private String coinbase;
    private long sequence;
    private String txid;
    private int vout;
    private ScriptSig scriptSig;
    private TransactionOutput spentOutput;
}
