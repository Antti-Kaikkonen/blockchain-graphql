package io.github.anttikaikkonen.blockchainanalyticsflink;

import io.github.anttikaikkonen.bitcoinrpcclientjava.RpcClient;
import java.io.Serializable;

public interface RpcClientBuilder extends Serializable {
    public RpcClient build();
}