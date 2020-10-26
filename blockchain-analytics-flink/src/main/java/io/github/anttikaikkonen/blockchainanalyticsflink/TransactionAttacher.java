package io.github.anttikaikkonen.blockchainanalyticsflink;

import io.github.anttikaikkonen.blockchainanalyticsflink.models.ConfirmedTransaction;
import io.github.anttikaikkonen.blockchainanalyticsflink.models.ConfirmedTransactionWithInputs;
import io.github.anttikaikkonen.blockchainanalyticsflink.models.TransactionInputWithOutput;
import io.github.anttikaikkonen.bitcoinrpcclientjava.models.TransactionInput;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;


public class TransactionAttacher extends KeyedCoProcessFunction<String, Tuple2<TransactionInputWithOutput, String>, ConfirmedTransaction, ConfirmedTransactionWithInputs> {

    private MapState<Long, ConfirmedTransaction> transactionState;
    private ListState<TransactionInputWithOutput> inputState;

    @Override
    public void open(Configuration parameters) throws Exception {
        this.inputState = getRuntimeContext().getListState(new ListStateDescriptor("inputs", TransactionInputWithOutput.class));
        this.transactionState = getRuntimeContext().getMapState(new MapStateDescriptor("transaction", Long.class, ConfirmedTransaction.class));
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<ConfirmedTransactionWithInputs> out) throws Exception {
        ConfirmedTransaction transaction = transactionState.get(timestamp);
        Map<String, TransactionInputWithOutput> inputMap = new HashMap<>();
        Iterable<TransactionInputWithOutput> inputs = inputState.get();
        if (inputs != null) {
            Iterator<TransactionInputWithOutput> iterator = inputs.iterator();
            while (iterator.hasNext()) {
                TransactionInputWithOutput input = iterator.next();
                inputMap.put(input.getTxid()+input.getVout(), input);
            }
        }
        if (transaction == null) {
            System.out.println("TRANSACTION NULL");
            for (String vin : inputMap.keySet()) {
                System.out.println(vin+ " = "+inputMap.get(vin));
            }
        //    return;
            //TODO bitcoin blocks 91722 and 91880 have the same coinbase transaction (same txid)
        }
        TransactionInputWithOutput[] res = new TransactionInputWithOutput[transaction.getVin().length];
        for (int i = 0; i < transaction.getVin().length; i++) {
            TransactionInput vin = transaction.getVin()[i];
            if (vin.getTxid() == null) {
                res[i] =  new TransactionInputWithOutput(vin, null);
            } else {
                res[i] = inputMap.get(vin.getTxid()+vin.getVout());
            }
        }
        //if (transaction.getTxN() == 0) {
            //Multiple coinbase transactions can have the same txid. For example the transactions below in BTC blockchain
            //d5d27987d2a3dfc724e359870c6644b40e497bdc0589a033220fe15429d88599 in blocks (91812 AND 91842)
            //e3bf3d07d4b0375638d5f1db5255fe07ba2c4cb067cd81b84ee974b6585fb468 in blocks (91722 AND 91880)
        //} else {
            transactionState.remove(timestamp);
            inputState.clear();
        //}
        out.collect(new ConfirmedTransactionWithInputs(transaction, res));
    }
    
    
    
    @Override
    public void processElement1(Tuple2<TransactionInputWithOutput, String> value, Context ctx, Collector<ConfirmedTransactionWithInputs> out) throws Exception {
        inputState.add(value.f0);
    }

    @Override
    public void processElement2(ConfirmedTransaction value, Context ctx, Collector<ConfirmedTransactionWithInputs> out) throws Exception {
        //ConfirmedTransaction oldTx = transactionState.value();
        //if (oldTx != null) System.out.println(oldTx.getTxid()+ " already exists! Height = "+ oldTx.getHeight());
        //transactionState.update(value);
        transactionState.put(ctx.timestamp(), value);
        ctx.timerService().registerEventTimeTimer(ctx.timestamp());
    }


    
    
    
}
