package io.github.anttikaikkonen.bitcoinrpcclientjava;

import io.github.anttikaikkonen.bitcoinrpcclientjava.models.Block;
import io.github.anttikaikkonen.bitcoinrpcclientjava.models.BlockHeader;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Semaphore;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;

public class LimitedCapacityRpcClient extends RpcClientImpl {
    
    private final Semaphore semaphore;
    
    public LimitedCapacityRpcClient(CloseableHttpAsyncClient client, String url, int capacity) {
        super(client, url);
        this.semaphore = new Semaphore(capacity, true);
    }
    
    private void acquire() {
        try {
            semaphore.acquire();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(ex);
        }
    }
    
    private void release() {
        semaphore.release();
    }
    
    @Override
    public CompletionStage<Integer> getBlockCount() {
        acquire();
        return super.getBlockCount().whenComplete((v, err) -> release());
    }

    @Override
    public CompletionStage<String> getBlockHash(int height) {
        acquire();
        return super.getBlockHash(height).whenComplete((v, err) -> release());
    }
    
    @Override
    public CompletionStage<Block> getBlock(String hash) {
        acquire();
        return super.getBlock(hash).whenComplete((v, err) -> release());
    }

    @Override
    public CompletionStage<BlockHeader> getBlockHeader(String fromHash) {
        acquire();
        return super.getBlockHeader(fromHash).whenComplete((v, err) -> release());
    }
    
}
