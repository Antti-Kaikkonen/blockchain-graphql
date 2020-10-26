package io.github.anttikaikkonen.blockchainanalyticsflink.casssandra;


import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.google.common.util.concurrent.ListenableFuture;
import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.AddressTransaction;
import org.apache.flink.configuration.Configuration;

public class AddressTransactionSaver extends CassandraSaverFunction<AddressTransaction> {

    private Mapper<AddressTransaction> mapper;
    
    public AddressTransactionSaver(CassandraSessionBuilder sessionBuilder) {
        super(sessionBuilder);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }


    @Override
    public ListenableFuture saveAsync(AddressTransaction input) {
        return this.mapper.saveAsync(input);
    }

    @Override
    public void initMappers(MappingManager manager) {
        this.mapper = manager.mapper(AddressTransaction.class);
    }
    
}
