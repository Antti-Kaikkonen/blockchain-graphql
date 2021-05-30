package io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models;

import com.datastax.driver.mapping.annotations.UDT;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@UDT(name = "scriptsig")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ScriptSig {

    public ScriptSig(io.github.anttikaikkonen.bitcoinrpcclientjava.models.ScriptSig rpcScriptSig) {
        this.asm = rpcScriptSig.getAsm();
        this.hex = rpcScriptSig.getHex();
    }

    String asm;
    String hex;
}
