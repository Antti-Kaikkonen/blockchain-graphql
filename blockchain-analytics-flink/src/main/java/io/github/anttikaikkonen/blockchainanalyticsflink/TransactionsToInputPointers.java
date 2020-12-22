package io.github.anttikaikkonen.blockchainanalyticsflink;

import io.github.anttikaikkonen.blockchainanalyticsflink.models.ConfirmedTransaction;
import io.github.anttikaikkonen.blockchainanalyticsflink.models.InputPointer;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class TransactionsToInputPointers implements FlatMapFunction<ConfirmedTransaction, InputPointer> {
    @Override
    public void flatMap(ConfirmedTransaction value, Collector<InputPointer> out) throws Exception {
        if (value.getTxN() == 0) return;

        for (int i = 0; i < value.getVin().length; i++) {
            out.collect(new InputPointer(value.getTxid(), value.getVin()[i].getTxid(), value.getVin()[i].getVout(), i));
        }
    }
}
