package io.github.anttikaikkonen.blockchainanalyticsflink.statefun.sink;

import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

public class ProcessingTimeToEventTime<E> implements WatermarkStrategy<E> {

    private long offset;

    public ProcessingTimeToEventTime(long offset) {
        this.offset = offset;
    }

    @Override
    public WatermarkGenerator<E> createWatermarkGenerator(WatermarkGeneratorSupplier.Context ctx) {
        return new WatermarkGenerator<E>() {
            @Override
            public void onEvent(E in, long timestamp, WatermarkOutput wo) {
                wo.emitWatermark(new Watermark(timestamp - 1));
                wo.markIdle();
            }

            @Override
            public void onPeriodicEmit(WatermarkOutput wo) {
                wo.emitWatermark(new Watermark(System.currentTimeMillis() + offset - 1));
            }

        };
    }

    @Override
    public TimestampAssigner<E> createTimestampAssigner(TimestampAssignerSupplier.Context ctx) {
        return new TimestampAssigner<E>() {
            @Override
            public long extractTimestamp(E in, long timestamp) {
                long time = System.currentTimeMillis() + offset;
                return time;
            }
        };
    }

}
