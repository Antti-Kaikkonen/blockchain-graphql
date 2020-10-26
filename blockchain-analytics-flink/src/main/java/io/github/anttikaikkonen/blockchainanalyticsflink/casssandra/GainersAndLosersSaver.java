package io.github.anttikaikkonen.blockchainanalyticsflink.casssandra;

import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.google.common.util.concurrent.ListenableFuture;
import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.TopGainers;
import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.TopLosers;
import io.github.anttikaikkonen.blockchainanalyticsflink.models.AddressBalanceChange;


public class GainersAndLosersSaver extends CassandraSaverFunction<AddressBalanceChange> {

    Mapper<TopGainers> gainersMapper;
    Mapper<TopLosers> losersMapper;
    
    public GainersAndLosersSaver(CassandraSessionBuilder sessionBuilder) {
        super(sessionBuilder);
    }

    @Override
    public ListenableFuture saveAsync(AddressBalanceChange input) {
        if (input.getBalanceChange() < 0) {
            TopLosers loser = new TopLosers(input.getDate(), input.getAddress(), input.getBalanceChange());
            return losersMapper.saveAsync(loser);
        } else {
            TopGainers gainer = new TopGainers(input.getDate(), input.getAddress(), input.getBalanceChange());
            return gainersMapper.saveAsync(gainer);
        }
    }

    @Override
    public void initMappers(MappingManager manager) {
        this.gainersMapper = manager.mapper(TopGainers.class);
        this.losersMapper = manager.mapper(TopLosers.class);
    }
    
}
