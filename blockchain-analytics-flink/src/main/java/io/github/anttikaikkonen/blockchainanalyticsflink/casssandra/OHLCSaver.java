package io.github.anttikaikkonen.blockchainanalyticsflink.casssandra;


import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.google.common.util.concurrent.ListenableFuture;
import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.OHLC;

public class OHLCSaver extends CassandraSaverFunction<OHLC> {

    private Mapper<OHLC> mapper;
    
    public OHLCSaver(CassandraSessionBuilder sessionBuilder) {
        super(sessionBuilder);
    }

    @Override
    public ListenableFuture saveAsync(OHLC input) {
        return this.mapper.saveAsync(input);
    }

    @Override
    public void initMappers(MappingManager manager) {
        this.mapper = manager.mapper(OHLC.class);
    }
    
}
