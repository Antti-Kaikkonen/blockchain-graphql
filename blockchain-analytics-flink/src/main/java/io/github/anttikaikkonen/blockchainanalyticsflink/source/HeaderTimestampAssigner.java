package io.github.anttikaikkonen.blockchainanalyticsflink.source;

import io.github.anttikaikkonen.bitcoinrpcclientjava.models.BlockHeader;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

public class HeaderTimestampAssigner implements WatermarkStrategy<BlockHeader> {
    
    public HeaderTimestampAssigner() {
    }
    
    @Override
    public WatermarkGenerator<BlockHeader> createWatermarkGenerator(WatermarkGeneratorSupplier.Context ctx) {
        return new WatermarkGenerator<BlockHeader>() {
            @Override
            public void onEvent(BlockHeader header, long timestamp, WatermarkOutput wo) {
                wo.emitWatermark(new Watermark(header.getTime()));
            }

            @Override
            public void onPeriodicEmit(WatermarkOutput arg0) {
            }
        };
    }

    @Override
    public TimestampAssigner<BlockHeader> createTimestampAssigner(TimestampAssignerSupplier.Context ctx) {
        return new TimestampAssigner<BlockHeader>() {
            @Override
            public long extractTimestamp(BlockHeader blockHeader, long timestamp) {
                return blockHeader.getTime();
            }
        };
    }

    
    
    
    
}
