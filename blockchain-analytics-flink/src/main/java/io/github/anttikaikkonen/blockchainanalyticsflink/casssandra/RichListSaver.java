package io.github.anttikaikkonen.blockchainanalyticsflink.casssandra;

import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.google.common.util.concurrent.ListenableFuture;
import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.RichList;


public class RichListSaver extends CassandraSaverFunction<RichList> {

    Mapper<RichList> mapper;
    
    public RichListSaver(CassandraSessionBuilder sessionBuilder) {
        super(sessionBuilder);
    }
    

    @Override
    public ListenableFuture saveAsync(RichList input) {
        return this.mapper.saveAsync(input);
    }

    @Override
    public void initMappers(MappingManager manager) {
        this.mapper = manager.mapper(RichList.class);
    }
    
}
