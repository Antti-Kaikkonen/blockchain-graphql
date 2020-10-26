package io.github.anttikaikkonen.bitcoinrpcclientjava;

import io.github.anttikaikkonen.bitcoinrpcclientjava.models.Block;
import io.github.anttikaikkonen.bitcoinrpcclientjava.models.BlockHeader;
import java.util.concurrent.CompletionStage;

public interface RpcClient {
    
    public CompletionStage<Integer> getBlockCount();

    public CompletionStage<String> getBlockHash(int height);
    
    public CompletionStage<Block> getBlock(String hash);

    public CompletionStage<BlockHeader> getBlockHeader(String fromHash);
    
    public void close();
    
}
