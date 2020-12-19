package io.github.anttikaikkonen.bitcoinrpcclientjava.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ScriptPubKey {
    
    private String asm;
    private String hex;
    private int reqSigs;
    private String type;
    private String[] addresses;
    
}
