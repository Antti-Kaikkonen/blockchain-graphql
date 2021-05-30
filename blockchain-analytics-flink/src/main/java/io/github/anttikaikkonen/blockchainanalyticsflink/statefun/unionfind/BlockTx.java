package io.github.anttikaikkonen.blockchainanalyticsflink.statefun.unionfind;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@EqualsAndHashCode
@AllArgsConstructor
@NoArgsConstructor
public class BlockTx {

    int txN;
    long delta;
}
