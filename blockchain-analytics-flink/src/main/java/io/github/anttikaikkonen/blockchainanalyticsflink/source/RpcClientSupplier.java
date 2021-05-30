package io.github.anttikaikkonen.blockchainanalyticsflink.source;

import io.github.anttikaikkonen.bitcoinrpcclientjava.LeastConnectionsRpcClient;
import io.github.anttikaikkonen.bitcoinrpcclientjava.RpcClient;
import io.github.anttikaikkonen.bitcoinrpcclientjava.RpcClientImpl;
import java.io.Serializable;
import java.util.function.Supplier;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.reactor.IOReactorConfig;

@AllArgsConstructor
@Getter
public class RpcClientSupplier implements Supplier<RpcClient>, Serializable {

    private final String blockchainUsername;
    private final String blockchainPassword;
    private final String[] blockchainRpcURLS;
    private final int maxConnectionsPerRoute;
    private final int maxConnectionsTotal;

    @Override
    public RpcClient get() {
        CredentialsProvider provider = new BasicCredentialsProvider();
        provider.setCredentials(
                AuthScope.ANY,
                new UsernamePasswordCredentials(blockchainUsername, blockchainPassword)
        );

        IOReactorConfig ioReactorConfig = IOReactorConfig.custom()
                .setIoThreadCount(1)
                .build();

        CloseableHttpAsyncClient httpClient = HttpAsyncClients.custom()
                .setDefaultIOReactorConfig(ioReactorConfig)
                .setDefaultCredentialsProvider(provider)
                .setMaxConnPerRoute(maxConnectionsPerRoute)
                .setMaxConnTotal(maxConnectionsTotal)
                .build();
        httpClient.start();

        RpcClient[] clients = new RpcClient[blockchainRpcURLS.length];
        for (int i = 0; i < clients.length; i++) {
            clients[i] = new RpcClientImpl(httpClient, blockchainRpcURLS[i].trim());//new LimitedCapacityRpcClient(httpClient, blockchainRpcURLS[i].trim(), parallelism);
        }
        return new LeastConnectionsRpcClient(clients);
    }

}
