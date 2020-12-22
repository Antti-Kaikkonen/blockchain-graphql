package io.github.anttikaikkonen.blockchainanalyticsflink;

import io.github.anttikaikkonen.bitcoinrpcclientjava.models.Block;
import io.github.anttikaikkonen.bitcoinrpcclientjava.models.Transaction;
import io.github.anttikaikkonen.blockchainanalyticsflink.models.ConfirmedTransaction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class BlocksToTransactions implements FlatMapFunction<Block, ConfirmedTransaction> {
    @Override
    public void flatMap(Block value, Collector<ConfirmedTransaction> out) throws Exception {
        int txN = 0;
        for (Transaction tx : value.getTx()) {
            out.collect(new ConfirmedTransaction(tx, value.getHeight(), txN));
            txN++;
        }
    }
}
