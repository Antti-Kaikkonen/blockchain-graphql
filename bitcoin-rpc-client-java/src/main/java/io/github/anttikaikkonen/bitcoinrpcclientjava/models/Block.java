package io.github.anttikaikkonen.bitcoinrpcclientjava.models;

import java.math.BigDecimal;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Block {
    
    String hash;
    int size;
    int height;
    int version;
    String versionHex;
    String merkleroot;
    Transaction[] tx;
    long time;
    long mediantime;
    long nonce;
    String bits;
    BigDecimal difficulty;
    String chainwork;
    String previousblockhash;
    
}