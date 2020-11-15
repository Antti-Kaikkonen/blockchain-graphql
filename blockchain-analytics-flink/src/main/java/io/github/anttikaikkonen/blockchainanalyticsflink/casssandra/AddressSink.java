package io.github.anttikaikkonen.blockchainanalyticsflink.casssandra;

import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.google.common.util.concurrent.ListenableFuture;
import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.AddressBalance;
import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.OHLC;
import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.RichList;
import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.TopGainers;
import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.TopLosers;

public class AddressSink extends CassandraSaverFunction<Object> {
    private Mapper<OHLC> ohlcMapper;
    private Mapper<TopGainers> gainersMapper;
    private Mapper<TopLosers> losersMapper;
    private Mapper<RichList> richListMapper;
    private Mapper<AddressBalance> addressBalanceMapper;
    
    public AddressSink(CassandraSessionBuilder sessionBuilder) {
        super(sessionBuilder);
    }
    
    @Override
    public ListenableFuture saveAsync(Object input) {
        if (input instanceof OHLC) {
            return this.ohlcMapper.saveAsync((OHLC) input);
        } else if (input instanceof TopGainers) {
            return this.gainersMapper.saveAsync((TopGainers) input);
        } else if (input instanceof TopLosers) {
            return this.losersMapper.saveAsync((TopLosers) input);
        } else if (input instanceof RichList) {
            return this.richListMapper.saveAsync((RichList) input);
        } else if (input instanceof AddressBalance) {
            return this.addressBalanceMapper.saveAsync((AddressBalance) input);
        } else {
            throw new RuntimeException("Unsupported type");
        }
    }

    @Override
    public void initMappers(MappingManager manager) {
        this.ohlcMapper = manager.mapper(OHLC.class);
        this.gainersMapper = manager.mapper(TopGainers.class);
        this.losersMapper = manager.mapper(TopLosers.class);
        this.richListMapper = manager.mapper(RichList.class);
        this.addressBalanceMapper = manager.mapper(AddressBalance.class);
    }



}
