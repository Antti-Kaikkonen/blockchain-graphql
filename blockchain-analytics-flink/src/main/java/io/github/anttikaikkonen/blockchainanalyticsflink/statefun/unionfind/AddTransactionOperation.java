package io.github.anttikaikkonen.blockchainanalyticsflink.statefun.unionfind;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class AddTransactionOperation {

    long time;
    int height;
    int tx_n;
    long delta;
}
