package io.github.anttikaikkonen.blockchainanalyticsflink.precluster;

import io.github.anttikaikkonen.blockchainanalyticsflink.statefun.unionfind.BlockTx;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class SimpleAddAddressesAndTransactionsOperation {
    String[] addresses;
    BlockTx[] blockTxs;
}
