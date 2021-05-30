package io.github.anttikaikkonen.bitcoinrpcclientjava.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Transaction {

    private String txid;
    private int size;
    private long version;
    private long locktime;
    private TransactionInput[] vin;
    private TransactionOutput[] vout;

}
