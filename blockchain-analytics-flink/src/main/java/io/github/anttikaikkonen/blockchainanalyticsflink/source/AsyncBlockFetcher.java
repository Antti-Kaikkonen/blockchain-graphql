package io.github.anttikaikkonen.blockchainanalyticsflink.source;

import io.github.anttikaikkonen.bitcoinrpcclientjava.RpcClient;
import io.github.anttikaikkonen.bitcoinrpcclientjava.models.Block;
import java.util.Collections;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

public class AsyncBlockFetcher extends RichAsyncFunction<String, Block> {

    private transient RpcClient client = null;
    private final Supplier<RpcClient> rpcClientSupplier;

    public AsyncBlockFetcher(Supplier<RpcClient> rpcClientSupplier) {
        this.rpcClientSupplier = rpcClientSupplier;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.client = this.rpcClientSupplier.get();
    }

    @Override
    public void close() throws Exception {
        this.client.close();
        this.client = null;
    }

    @Override
    public void asyncInvoke(String input, ResultFuture<Block> resultFuture) throws Exception {
        CompletionStage<Block> blockRpc = client.getBlock(input);
        blockRpc.whenCompleteAsync(new BiConsumer<Block, Throwable>() {
            @Override
            public void accept(Block block, Throwable err) {
                if (block == null) {
                    resultFuture.completeExceptionally(err);
                } else {
                    resultFuture.complete(Collections.singleton(block));
                }
            }
        }, org.apache.flink.runtime.concurrent.Executors.directExecutor());
    }

}
