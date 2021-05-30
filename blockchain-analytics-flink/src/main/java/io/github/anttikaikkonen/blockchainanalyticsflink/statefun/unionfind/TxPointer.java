package io.github.anttikaikkonen.blockchainanalyticsflink.statefun.unionfind;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode
public class TxPointer {

    long time;
    int height;
    int tx_n;
}
