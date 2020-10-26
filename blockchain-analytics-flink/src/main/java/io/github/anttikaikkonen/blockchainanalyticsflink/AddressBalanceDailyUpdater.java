package io.github.anttikaikkonen.blockchainanalyticsflink;

import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.RichList;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class AddressBalanceDailyUpdater extends KeyedProcessFunction<String, Tuple2<String, Long>, RichList>{

    MapState<Long, Long> timestampDelta;
    ValueState<Long> balanceState;
    private long previousTimestamp = 0;
    
    private final long minimumBalance;
    
    public AddressBalanceDailyUpdater(long minimumBalance) {
        this.minimumBalance = minimumBalance;
    }

    @Override
    public void open(Configuration config) {
        this.timestampDelta = getRuntimeContext().getMapState(new MapStateDescriptor<>("asd", Long.class, Long.class));
        this.balanceState = getRuntimeContext().getState(new ValueStateDescriptor<>("balance", Long.class));
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<RichList> out) throws Exception {
        if (timestamp < previousTimestamp) {
            System.out.println("DECREASED BY "+(previousTimestamp-timestamp) +" ("+previousTimestamp+" > "+timestamp+")");
        }
        previousTimestamp = timestamp;
        Long delta = timestampDelta.get(timestamp);
        if (delta == null) {
            delta = 0l;
        }
        long oldBalance = balanceState.value() == null ? 0 : balanceState.value();
        long newBalance = oldBalance + delta;
        RichList addressBalanceUpdate = new RichList(ctx.getCurrentKey(), newBalance, oldBalance, ctx.timestamp());
        if (newBalance >= minimumBalance) {
            out.collect(addressBalanceUpdate);
        }
        balanceState.update(newBalance == 0 ? null : newBalance);
        timestampDelta.remove(timestamp);
        if (newBalance >= minimumBalance) {
            ctx.timerService().registerEventTimeTimer(ctx.timestamp()+1000*60*60*24);
        }
    }
    
    @Override
    public void processElement(Tuple2<String, Long> value, Context ctx, Collector<RichList> out) throws Exception {
        timestampDelta.put(ctx.timestamp(), value.f1);
        ctx.timerService().registerEventTimeTimer(ctx.timestamp());
        
    }
    
}
