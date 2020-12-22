package io.github.anttikaikkonen.blockchainanalyticsflink.source;

import io.github.anttikaikkonen.bitcoinrpcclientjava.RpcClient;
import java.util.Collections;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

public class AsyncBlockHashFetcher extends RichAsyncFunction<Integer, String> {

    private transient RpcClient client = null;
    private final Supplier<RpcClient> rpcClientSupplier;
    
    public AsyncBlockHashFetcher(Supplier<RpcClient> rpcClientSupplier) {
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
    public void asyncInvoke(Integer input, ResultFuture<String> resultFuture) throws Exception {
        CompletionStage<String> rpcHash = client.getBlockHash(input);
        rpcHash.whenCompleteAsync(new BiConsumer<String, Throwable>() {
            @Override
            public void accept(String hash, Throwable err) {
                if (hash == null) {
                    resultFuture.completeExceptionally(err);
                } else {
                    resultFuture.complete(Collections.singleton(hash));
                }
            }
        }, org.apache.flink.runtime.concurrent.Executors.directExecutor());
    }
}
