package io.github.anttikaikkonen.blockchainanalyticsflink;

import io.github.anttikaikkonen.bitcoinrpcclientjava.models.TransactionOutput;
import io.github.anttikaikkonen.blockchainanalyticsflink.models.ConfirmedTransactionWithInputs;
import io.github.anttikaikkonen.blockchainanalyticsflink.models.TransactionInputWithOutput;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class TransactionsToAddressBalanceChanges implements FlatMapFunction<ConfirmedTransactionWithInputs, Tuple2<String, Long>> {

    @Override
    public void flatMap(ConfirmedTransactionWithInputs transaction, Collector<Tuple2<String, Long>> out) throws Exception {

        Map<String, Long> addressDeltas = new HashMap<>();
        for (TransactionOutput vout : transaction.getVout()) {
            if (vout.getScriptPubKey().getAddresses() == null) continue;
            if (vout.getScriptPubKey().getAddresses().length != 1) continue;
            String address = vout.getScriptPubKey().getAddresses()[0];
            long value = Math.round(vout.getValue()*1e8);
            addressDeltas.compute(address, (key, oldDelta) -> oldDelta == null ? value : oldDelta + value);
        }
        for (TransactionInputWithOutput vin : transaction.getInputsWithOutputs()) {
            if (vin.getSpentOutput() == null) continue;
            if (vin.getSpentOutput().getScriptPubKey().getAddresses() == null) continue;
            if (vin.getSpentOutput().getScriptPubKey().getAddresses().length != 1) continue;
            String address = vin.getSpentOutput().getScriptPubKey().getAddresses()[0];
            long value = Math.round(vin.getSpentOutput().getValue()*1e8);
            addressDeltas.compute(address, (key, oldDelta) -> oldDelta == null ? -value : oldDelta - value);
        }
        for (Map.Entry<String, Long> e : addressDeltas.entrySet()) {
            out.collect(new Tuple2<String, Long>(e.getKey(), e.getValue()));
        }
    }
    
}
