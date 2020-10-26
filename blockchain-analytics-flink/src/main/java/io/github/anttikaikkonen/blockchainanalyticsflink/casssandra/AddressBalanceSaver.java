package io.github.anttikaikkonen.blockchainanalyticsflink.casssandra;

import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.google.common.util.concurrent.ListenableFuture;
import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.AddressBalance;
import io.github.anttikaikkonen.blockchainanalyticsflink.models.AddressBalanceUpdate;

public class AddressBalanceSaver extends CassandraSaverFunction<AddressBalanceUpdate> {

    Mapper<AddressBalance> mapper;
    
    public AddressBalanceSaver(CassandraSessionBuilder sessionBuilder) {
        super(sessionBuilder);
    }
    
    @Override
    public ListenableFuture saveAsync(AddressBalanceUpdate input) {
        return mapper.saveAsync(input);
    }

    @Override
    public void initMappers(MappingManager manager) {
        this.mapper = manager.mapper(AddressBalance.class);
    }
    
}