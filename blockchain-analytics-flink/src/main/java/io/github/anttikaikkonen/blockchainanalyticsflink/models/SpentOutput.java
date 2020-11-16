package io.github.anttikaikkonen.blockchainanalyticsflink.models;

import io.github.anttikaikkonen.bitcoinrpcclientjava.models.TransactionOutput;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SpentOutput {
    String spending_txid;
    //String txid;
    Integer input_index;
    TransactionOutput output;
}
