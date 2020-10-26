package io.github.anttikaikkonen.blockchainanalyticsflink;

import io.github.anttikaikkonen.blockchainanalyticsflink.models.TransactionInputWithOutput;
import io.github.anttikaikkonen.bitcoinrpcclientjava.models.TransactionInput;
import io.github.anttikaikkonen.bitcoinrpcclientjava.models.TransactionOutput;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

public class InputAttacher extends KeyedCoProcessFunction<String, Tuple2<TransactionInput, String>, Tuple2<TransactionOutput, String>, Tuple2<TransactionInputWithOutput, String>> implements CheckpointListener {

    private ValueState<Tuple2<TransactionInput, String>> inputState;
    private ValueState<TransactionOutput> outputState;
    
    @Override
    public void open(Configuration parameters) throws Exception {
        this.outputState = getRuntimeContext().getState(new ValueStateDescriptor("output", TransactionOutput.class));
        this.inputState = getRuntimeContext().getState(new ValueStateDescriptor("input", TypeInformation.of(new TypeHint<Tuple2<TransactionInput, String>>() {})));
    }
    

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<TransactionInputWithOutput, String>> out) throws Exception {
        Tuple2<TransactionInput, String> input = inputState.value();
        TransactionInputWithOutput transactionInputWithOutput = new TransactionInputWithOutput(input.f0, outputState.value());
        String txid = input.f1;
        inputState.clear();
        outputState.clear();
        out.collect(new Tuple2<>(transactionInputWithOutput, txid));
    }
    
    @Override
    public void processElement1(Tuple2<TransactionInput, String> value, Context ctx, Collector<Tuple2<TransactionInputWithOutput, String>> out) throws Exception {
        TransactionOutput output = outputState.value();
        if (output != null) {
            TransactionInputWithOutput transactionInputWithOutput = new TransactionInputWithOutput(value.f0, output);
            out.collect(new Tuple2<>(transactionInputWithOutput, value.f1));
            outputState.clear();
        } else {
            inputState.update(value);
            ctx.timerService().registerEventTimeTimer(ctx.timestamp());
        }
    }

    @Override
    public void processElement2(Tuple2<TransactionOutput, String> value, Context ctx, Collector<Tuple2<TransactionInputWithOutput, String>> out) throws Exception {
        outputState.update(value.f0);
    }

    @Override
    public void notifyCheckpointComplete(long arg0) throws Exception {
        System.out.println("InputAttacher checkpointCompleted!");
    }

}

