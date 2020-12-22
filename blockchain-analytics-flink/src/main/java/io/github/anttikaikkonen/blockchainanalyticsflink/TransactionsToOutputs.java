package io.github.anttikaikkonen.blockchainanalyticsflink;

import io.github.anttikaikkonen.bitcoinrpcclientjava.models.TransactionOutput;
import io.github.anttikaikkonen.blockchainanalyticsflink.models.ConfirmedTransaction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class TransactionsToOutputs implements FlatMapFunction<ConfirmedTransaction, Tuple2<TransactionOutput, String>> {
    @Override
    public void flatMap(ConfirmedTransaction value, Collector<Tuple2<TransactionOutput, String>> out) throws Exception {
        for (TransactionOutput vout : value.getVout()) {
            out.collect(new Tuple2(vout, value.getTxid()));
        }
    }
}
