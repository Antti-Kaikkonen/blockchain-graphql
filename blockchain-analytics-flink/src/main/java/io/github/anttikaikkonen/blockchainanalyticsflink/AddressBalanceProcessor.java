package io.github.anttikaikkonen.blockchainanalyticsflink;

import com.datastax.driver.core.LocalDate;
import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.AddressBalance;
import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.OHLC;
import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.RichList;
import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.TopGainers;
import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.TopLosers;
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

public class AddressBalanceProcessor extends KeyedProcessFunction<String, Tuple2<String, Long>, Object> {

    private static final long MILLIS_IN_DAY = 1000*60*60*24;
    
    ValueState<Long> balanceState;
    MapState<Long, Long> timeToBlockDelta;
    MapState<Long, OHLC> dateToOHLC;

    @Override
    public void open(Configuration parameters) throws Exception {
        this.timeToBlockDelta = getRuntimeContext().getMapState(new MapStateDescriptor<>("time_to_block_delta", Long.class, Long.class));
        this.balanceState = getRuntimeContext().getState(new ValueStateDescriptor<>("balance", Long.class));
        this.dateToOHLC = getRuntimeContext().getMapState(new MapStateDescriptor<>("date_to_ohlc", Long.class, OHLC.class));
    }
    
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Object> out) throws Exception {
        Long blockDelta = timeToBlockDelta.get(timestamp);
        Long oldBalance = balanceState.value();
        if (oldBalance == null) oldBalance = 0l;
        long epochDate = timestampToEpochDate(timestamp);
        OHLC ohlc = dateToOHLC.get(epochDate);
        if (ohlc == null) ohlc = new OHLC();
        long balance;
        if (blockDelta != null) {
            balance = oldBalance + blockDelta;
            balanceState.update(balance);
            AddressBalance addressBalance = new AddressBalance(ctx.getCurrentKey(), Date.from(Instant.ofEpochMilli(ctx.timestamp())), balance);
            out.collect(addressBalance);
            if (ohlc.getOpen() == null) {
                ohlc.setAddress(ctx.getCurrentKey());
                ohlc.setTimestamp(Date.from(Instant.ofEpochMilli(startOfDay(timestamp))));
                ohlc.setInterval((int)MILLIS_IN_DAY);
                //long previousBalance = value.getBalance()-value.getBalanceChange();
                ohlc.setOpen(oldBalance);
                ohlc.setHigh(Math.max(balance, oldBalance));
                ohlc.setLow(Math.min(balance, oldBalance));
            } else {
                if (balance > ohlc.getHigh()) {
                    ohlc.setHigh(balance);
                } else if (balance < ohlc.getLow()) {
                    ohlc.setLow(balance);
                }
            }
            ohlc.setClose(balance);
            dateToOHLC.put(epochDate, ohlc);
            timeToBlockDelta.remove(timestamp);
        } else {
            balance = oldBalance;
        }
        if (timestamp == endOfDay(timestamp)) {
            long dailyDelta;
            if (ohlc.getOpen() != null) {
                dailyDelta = ohlc.getClose()-ohlc.getOpen();
                out.collect(ohlc);
                dateToOHLC.remove(epochDate);
            } else {//No transactions but rich address
                dailyDelta = 0;
            }
            if (dailyDelta > 0) {
                int x = ctx.getCurrentKey().hashCode();
                /*Integer.valueOf(x).
                int r = x%256;
                if (r < 0) {
                    r -= 256; 
                }*/
                //int bin = ctx.getCurrentKey().hashCode()%256;
                //if (bin)
                TopGainers gainer = new TopGainers(LocalDate.fromDaysSinceEpoch((int)epochDate), ctx.getCurrentKey(), dailyDelta);
                out.collect(gainer);
            } else if (dailyDelta < 0) {
                TopLosers loser = new TopLosers(LocalDate.fromDaysSinceEpoch((int)epochDate), ctx.getCurrentKey(), dailyDelta);
                out.collect(loser);
            }
            if (balance > 100e8) {
                RichList rich = new RichList(ctx.getCurrentKey(), balance, balance-dailyDelta, timestamp);
                out.collect(rich);
                ctx.timerService().registerEventTimeTimer(timestamp+MILLIS_IN_DAY);//Process rich addresses every 24 hours
            }
            //emit OHLC etc
        }
    }
    
    private long timestampToEpochDate(long timestamp) {
        return timestamp/MILLIS_IN_DAY;
    }
    
    private long epochDateToTimeStamp(long epochDate) {
        return epochDate*MILLIS_IN_DAY;
    }
    
    private long endOfDay(long timestamp) {
        long day = timestampToEpochDate(timestamp);
        return epochDateToTimeStamp(day+1)-1;
    }
    
     private long startOfDay(long timestamp) {
        long day = timestampToEpochDate(timestamp);
        return epochDateToTimeStamp(day);
    }
    
    @Override
    public void processElement(Tuple2<String, Long> balanceChange, Context ctx, Collector<Object> arg2) throws Exception {
        Long oldDelta = timeToBlockDelta.get(ctx.timestamp());
        timeToBlockDelta.put(ctx.timestamp(), oldDelta == null ? balanceChange.f1 : oldDelta + balanceChange.f1);
        ctx.timerService().registerEventTimeTimer(endOfDay(ctx.timestamp()));
        
        ctx.timerService().registerEventTimeTimer(ctx.timestamp());
    }

}
