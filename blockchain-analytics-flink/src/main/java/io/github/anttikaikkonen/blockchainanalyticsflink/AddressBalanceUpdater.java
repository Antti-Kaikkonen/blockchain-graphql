package io.github.anttikaikkonen.blockchainanalyticsflink;

import io.github.anttikaikkonen.blockchainanalyticsflink.models.AddressBalanceUpdate;
import java.time.Instant;
import java.util.Date;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class AddressBalanceUpdater extends KeyedProcessFunction<String, Tuple2<String, Long>, AddressBalanceUpdate>{

    MapState<Long, Long> timestamp2Delta;
    ValueState<Long> balanceState;
    private long previousTimestamp = 0;

    @Override
    public void open(Configuration config) {
        this.timestamp2Delta = getRuntimeContext().getMapState(new MapStateDescriptor<>("asd", Long.class, Long.class));
        this.balanceState = getRuntimeContext().getState(new ValueStateDescriptor<>("balance", Long.class));
    }

    
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<AddressBalanceUpdate> out) throws Exception {
        if (timestamp < previousTimestamp) {
            System.out.println("DECREASED BY "+(previousTimestamp-timestamp) +" ("+previousTimestamp+" > "+timestamp+")");
        }
        previousTimestamp = timestamp;
        Long delta = timestamp2Delta.get(timestamp);
        long oldBalance = balanceState.value() == null ? 0 : balanceState.value();
        long newBalance = oldBalance + delta;
        AddressBalanceUpdate addressBalanceUpdate = new AddressBalanceUpdate(ctx.getCurrentKey(), newBalance, oldBalance, Date.from(Instant.ofEpochMilli(ctx.timestamp())));
        //RichList addressBalanceUpdate = new RichList(ctx.getCurrentKey(), newBalance, oldBalance, timestamp);
        out.collect(addressBalanceUpdate);
        balanceState.update(newBalance == 0 ? null : newBalance);
        timestamp2Delta.remove(timestamp);
    }
    
    
    
    @Override
    public void processElement(Tuple2<String, Long> value, Context ctx, Collector<AddressBalanceUpdate> out) throws Exception {
        
        timestamp2Delta.put(ctx.timestamp(), value.f1);
        ctx.timerService().registerEventTimeTimer(ctx.timestamp());
        
    }
    
}
