package io.github.anttikaikkonen.bitcoinrpcclientjava.models;

import java.math.BigDecimal;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class BlockHeader {
    
    String hash;
    int height;
    int confirmations;
    int version;
    String versionHex;
    String merkleroot;
    long time;
    long mediantime;
    long nonce;
    String bits;
    BigDecimal difficulty;
    String chainwork;
    String previousblockhash;
    String nextblockhash;
    
}
