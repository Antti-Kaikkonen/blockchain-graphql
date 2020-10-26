package io.github.anttikaikkonen.blockchainanalyticsflink.models;

import io.github.anttikaikkonen.bitcoinrpcclientjava.models.TransactionInput;
import io.github.anttikaikkonen.bitcoinrpcclientjava.models.TransactionOutput;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TransactionInputWithOutput extends TransactionInput {
    
    public TransactionInputWithOutput(TransactionInput input, TransactionOutput spentOutput) {
        setCoinbase(input.getCoinbase());
        setSequence(input.getSequence());
        setTxid(input.getTxid());
        setVout(input.getVout());
        setScriptSig(input.getScriptSig());
        setSpentOutput(spentOutput);
    }

    TransactionOutput spentOutput;
}
