package io.github.anttikaikkonen.blockchainanalyticsflink.precluster;

import io.github.anttikaikkonen.bitcoinrpcclientjava.models.TransactionOutput;
import io.github.anttikaikkonen.blockchainanalyticsflink.models.ConfirmedTransactionWithInputs;
import io.github.anttikaikkonen.blockchainanalyticsflink.models.TransactionInputWithOutput;
import java.util.Set;
import java.util.TreeSet;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple2;

public class TransactionClustering extends ProcessFunction<ConfirmedTransactionWithInputs, Tuple2<Integer, SimpleAddAddressesAndTransactionsOperation[]>> {

    @Override
    public void processElement(ConfirmedTransactionWithInputs tx, Context ctx, Collector<Tuple2<Integer, SimpleAddAddressesAndTransactionsOperation[]>> out) throws Exception {
        //TxPointer txP = new TxPointer(ctx.timestamp(), tx.getHeight(), tx.getTxN());
        DisjointSetForest unionFind = new DisjointSetForest();
        for (TransactionInputWithOutput vin : tx.getInputsWithOutputs()) {
            try {
                String address = vin.getSpentOutput().getScriptPubKey().getAddresses()[0];
                unionFind.addTx(address, tx.getTxN(), Math.round(-vin.getSpentOutput().getValue()*1e8));
            } catch(Exception ex) {
            }
        }
        for (TransactionOutput vout : tx.getVout()) {
            try {
                String address = vout.getScriptPubKey().getAddresses()[0];
                unionFind.addTx(address, tx.getTxN(), Math.round(vout.getValue()*1e8));
            } catch(Exception ex) {
            }
        }
        if (!tx.possiblyCoinJoin()) {
            Set<String> inputAddresses = new TreeSet<>();
            for (TransactionInputWithOutput vin : tx.getInputsWithOutputs()) {
                try {
                    String address = vin.getSpentOutput().getScriptPubKey().getAddresses()[0];
                    if (address != null) {
                        inputAddresses.add(address);
                    }
                } catch(Exception ex) {
                }
            }
            String firstAddress = null;
            for (String address : inputAddresses) {
                unionFind.makeSet(address);
                if (firstAddress == null) {
                    firstAddress = address;
                } else {
                    unionFind.union(address, firstAddress);
                }
            }
        }
        out.collect(Tuple2.of(tx.getHeight(), unionFind.toAddOps()));
    }
    
}
