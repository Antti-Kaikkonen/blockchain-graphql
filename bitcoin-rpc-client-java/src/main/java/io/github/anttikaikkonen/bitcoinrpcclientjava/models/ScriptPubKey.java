package io.github.anttikaikkonen.bitcoinrpcclientjava.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ScriptPubKey {
    
    String asm;
    String hex;
    int reqSigs;
    String type;
    String[] addresses;
    
}
