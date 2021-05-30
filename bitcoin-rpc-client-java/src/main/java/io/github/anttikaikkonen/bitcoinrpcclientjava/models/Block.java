package io.github.anttikaikkonen.bitcoinrpcclientjava.models;

import java.math.BigDecimal;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Block {

    private String hash;
    private int size;
    private int height;
    private int version;
    private String versionHex;
    private String merkleroot;
    private Transaction[] tx;
    private long time;
    private long mediantime;
    private long nonce;
    private String bits;
    private BigDecimal difficulty;
    private String chainwork;
    private String previousblockhash;

}
