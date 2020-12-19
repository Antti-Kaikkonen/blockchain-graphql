package io.github.anttikaikkonen.bitcoinrpcclientjava.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TransactionInput {
    
    private String coinbase;
    private long sequence;
    private String txid;
    private int vout;
    private ScriptSig scriptSig;
    
}
