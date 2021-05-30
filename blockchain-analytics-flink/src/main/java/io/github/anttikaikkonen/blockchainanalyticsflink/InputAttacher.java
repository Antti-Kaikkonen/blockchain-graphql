package io.github.anttikaikkonen.blockchainanalyticsflink;

import io.github.anttikaikkonen.bitcoinrpcclientjava.models.TransactionOutput;
import io.github.anttikaikkonen.blockchainanalyticsflink.models.InputPointer;
import io.github.anttikaikkonen.blockchainanalyticsflink.models.SpentOutput;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

public class InputAttacher extends KeyedCoProcessFunction<String, InputPointer, Tuple2<TransactionOutput, String>, SpentOutput> {

    private ValueState<InputPointer> inputState;
    private ValueState<TransactionOutput> outputState;

    @Override
    public void open(Configuration parameters) throws Exception {
        this.outputState = getRuntimeContext().getState(new ValueStateDescriptor("output", TransactionOutput.class));
        this.inputState = getRuntimeContext().getState(new ValueStateDescriptor("input", InputPointer.class));
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<SpentOutput> out) throws Exception {
        InputPointer inputPointer = inputState.value();
        SpentOutput spentOutput = new SpentOutput(inputPointer.getSpending_txid(), inputPointer.getInput_index(), outputState.value());
        inputState.clear();
        outputState.clear();
        out.collect(spentOutput);
    }

    @Override
    public void processElement1(InputPointer inputPointer, Context ctx, Collector<SpentOutput> out) throws Exception {
        TransactionOutput output = outputState.value();
        if (output != null) {
            SpentOutput spentOutput = new SpentOutput(inputPointer.getSpending_txid(), inputPointer.getInput_index(), output);
            out.collect(spentOutput);
            outputState.clear();
        } else {
            inputState.update(inputPointer);
            ctx.timerService().registerEventTimeTimer(ctx.timestamp());
        }
    }

    @Override
    public void processElement2(Tuple2<TransactionOutput, String> value, Context ctx, Collector<SpentOutput> out) throws Exception {
        outputState.update(value.f0);
    }

}
