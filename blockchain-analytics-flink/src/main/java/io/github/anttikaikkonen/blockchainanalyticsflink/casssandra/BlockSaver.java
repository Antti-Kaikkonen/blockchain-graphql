package io.github.anttikaikkonen.blockchainanalyticsflink.casssandra;


import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.LongestChain;
import io.github.anttikaikkonen.bitcoinrpcclientjava.models.Block;
import java.util.List;

public class BlockSaver extends CassandraSaverFunction<Block> {

    
    private Mapper<io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.Block> blockMapper;
    private Mapper<LongestChain> heightMapper;
    
    public BlockSaver(CassandraSessionBuilder sessionBuilder) {
        super(sessionBuilder);
    }
    
    @Override
    public ListenableFuture saveAsync(Block block) {
        io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.Block block2 = new io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.Block(block);
        ListenableFuture<Void> blockFuture = blockMapper.saveAsync(block2);
        LongestChain blockHeight = new LongestChain(block.getHeight(), block.getHash());
        ListenableFuture<Void> blockHeightFuture = heightMapper.saveAsync(blockHeight);
        ListenableFuture<List<Void>> res = Futures.allAsList(blockFuture, blockHeightFuture);
        return res;
    }

    @Override
    public void initMappers(MappingManager manager) {
        this.heightMapper = manager.mapper(LongestChain.class);
        this.blockMapper = manager.mapper(io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.Block.class);
    }
    
}
