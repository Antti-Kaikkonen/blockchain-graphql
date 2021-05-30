package io.github.anttikaikkonen.blockchainanalyticsflink;

import io.github.anttikaikkonen.bitcoinrpcclientjava.models.TransactionInput;
import io.github.anttikaikkonen.bitcoinrpcclientjava.models.TransactionOutput;
import io.github.anttikaikkonen.blockchainanalyticsflink.models.ConfirmedTransaction;
import io.github.anttikaikkonen.blockchainanalyticsflink.models.ConfirmedTransactionWithInputs;
import io.github.anttikaikkonen.blockchainanalyticsflink.models.SpentOutput;
import io.github.anttikaikkonen.blockchainanalyticsflink.models.TransactionInputWithOutput;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;


public class TransactionAttacher extends KeyedCoProcessFunction<String, SpentOutput, ConfirmedTransaction, ConfirmedTransactionWithInputs> {

    //We store transaction by time because multiple blocks can have a coinbase transaction with the same txid. Exaxmple: BTC block 91722 and 91880
    private MapState<Long, ConfirmedTransaction> transactionState;
    private ListState<SpentOutput> inputState;

    @Override
    public void open(Configuration parameters) throws Exception {
        this.inputState = getRuntimeContext().getListState(new ListStateDescriptor("inputs", SpentOutput.class));
        this.transactionState = getRuntimeContext().getMapState(new MapStateDescriptor("transaction", Long.class, ConfirmedTransaction.class));
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<ConfirmedTransactionWithInputs> out) throws Exception {
        ConfirmedTransaction transaction = transactionState.get(timestamp);
        Map<Integer, TransactionOutput> outputsByInputIndex = new HashMap<>();
        Iterable<SpentOutput> inputs = inputState.get();
        if (inputs != null) {
            Iterator<SpentOutput> iterator = inputs.iterator();
            while (iterator.hasNext()) {
                SpentOutput spentOutput = iterator.next();
                outputsByInputIndex.put(spentOutput.getInput_index(), spentOutput.getOutput());
            }
        }
        TransactionInputWithOutput[] inputsWithOutputs = new TransactionInputWithOutput[transaction.getVin().length];
        for (int i = 0; i < transaction.getVin().length; i++) {
            TransactionInput vin = transaction.getVin()[i];
            if (vin.getTxid() == null) {
                throw new Exception("VIN TXID NULL");
                //inputsWithOutputs[i] =  new TransactionInputWithOutput(vin, null);
            } else {
                inputsWithOutputs[i] = new TransactionInputWithOutput(vin, outputsByInputIndex.get(i));
            }
        }
        transactionState.remove(timestamp);
        inputState.clear();
        out.collect(new ConfirmedTransactionWithInputs(transaction, inputsWithOutputs, timestamp));
    }
    
    @Override
    public void processElement1(SpentOutput value, Context ctx, Collector<ConfirmedTransactionWithInputs> out) throws Exception {
        inputState.add(value);
    }

    @Override
    public void processElement2(ConfirmedTransaction transaction, Context ctx, Collector<ConfirmedTransactionWithInputs> out) throws Exception {
        if (transaction.getTxN() == 0) {
            TransactionInputWithOutput[] inputs = new TransactionInputWithOutput[transaction.getVin().length];
            inputs[0] = new TransactionInputWithOutput(transaction.getVin()[0], null);
            out.collect(new ConfirmedTransactionWithInputs(transaction, inputs, ctx.timestamp()));
        } else {
            transactionState.put(ctx.timestamp(), transaction);
            ctx.timerService().registerEventTimeTimer(ctx.timestamp());
        }
    }
    
}
