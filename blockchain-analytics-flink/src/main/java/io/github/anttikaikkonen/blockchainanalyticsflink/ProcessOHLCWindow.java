package io.github.anttikaikkonen.blockchainanalyticsflink;

import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.OHLC;
import java.time.Instant;
import java.util.Date;
import java.util.Iterator;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


public class ProcessOHLCWindow extends ProcessWindowFunction<OHLC, OHLC, String, TimeWindow> {

    @Override
    public void process(String key, Context context, Iterable<OHLC> elements, Collector<OHLC> out) throws Exception {
        Iterator<OHLC> iterator = elements.iterator();
        OHLC res = iterator.next();
        res.setTimestamp(Date.from(Instant.ofEpochMilli(context.window().getStart())));
        res.setInterval((int)(context.window().getEnd()-context.window().getStart()));
        out.collect(res);
    }
    
}
