package io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models;

import com.datastax.driver.mapping.annotations.Frozen;
import com.datastax.driver.mapping.annotations.UDT;
import java.util.Arrays;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@UDT(name = "scriptpubkey")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ScriptPubKey {

    public ScriptPubKey(io.github.anttikaikkonen.bitcoinrpcclientjava.models.ScriptPubKey rpcPubkey) {
        this.asm = rpcPubkey.getAsm();
        this.hex = rpcPubkey.getHex();
        this.reqSigs = rpcPubkey.getReqSigs();
        this.type = rpcPubkey.getType();
        if (rpcPubkey.getAddresses() != null) {
            this.addresses = Arrays.asList(rpcPubkey.getAddresses());
        }
    }

    String asm;
    String hex;
    int reqSigs;
    String type;
    @Frozen
    List<String> addresses;

}
