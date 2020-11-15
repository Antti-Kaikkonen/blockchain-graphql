package io.github.anttikaikkonen.blockchainanalyticsflink.statefun.unionfind;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class AddAddressesAndTransactionsOperation {
    String[] addresses;
    int height;
    long timestamp;
    BlockTx[] blockTxs;
}
