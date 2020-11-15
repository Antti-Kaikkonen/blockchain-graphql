package io.github.anttikaikkonen.blockchainanalyticsflink.source;

import io.github.anttikaikkonen.bitcoinrpcclientjava.RpcClient;
import io.github.anttikaikkonen.bitcoinrpcclientjava.models.BlockHeader;
import io.github.anttikaikkonen.blockchainanalyticsflink.RpcClientBuilder;
import java.util.Collections;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

public class AsyncBlockHeadersFetcher extends RichAsyncFunction<String, BlockHeader> {

    private transient RpcClient client = null;
    private final RpcClientBuilder rpcClientBuilder;
    
    public AsyncBlockHeadersFetcher(RpcClientBuilder rpcClientBuilder) {
        this.rpcClientBuilder = rpcClientBuilder;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.client = this.rpcClientBuilder.build();
    }
    
    
    @Override
    public void close() throws Exception {
        this.client = null;
    }

    
    @Override
    public void asyncInvoke(String input, ResultFuture<BlockHeader> resultFuture) throws Exception {
        CompletionStage<BlockHeader> rpcHeader = client.getBlockHeader(input);
        rpcHeader.whenCompleteAsync(new BiConsumer<BlockHeader, Throwable>() {
            @Override
            public void accept(BlockHeader header, Throwable err) {
                if (header == null) {
                    resultFuture.completeExceptionally(err);
                } else {
                    resultFuture.complete(Collections.singleton(header));
                }
            }
        }, org.apache.flink.runtime.concurrent.Executors.directExecutor());
    }

}
