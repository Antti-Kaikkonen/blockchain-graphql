package io.github.anttikaikkonen.bitcoinrpcclientjava;

import io.github.anttikaikkonen.bitcoinrpcclientjava.models.Block;
import io.github.anttikaikkonen.bitcoinrpcclientjava.models.BlockHeader;
import java.io.Serializable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

public class LeastConnectionsRpcClient implements RpcClient {

    private final RpcClient[] clients;

    private final PriorityBlockingQueue<RpcClientWithConnections> queue;

    private long currentAge = 0;

    private transient ScheduledExecutorService bannedNodeChecker = Executors.newSingleThreadScheduledExecutor();

    public LeastConnectionsRpcClient(RpcClient[] clients) {
        this.clients = clients;
        this.queue = new PriorityBlockingQueue<>(clients.length);
        for (int i = 0; i < clients.length; i++) {
            queue.add(new RpcClientWithConnections(clients[i], currentAge));
            this.currentAge++;
        }
    }

    @Override
    public CompletionStage<Integer> getBlockCount() {
        return new TryAllAvailableHosts<>((RpcClient client) -> client.getBlockCount()).get();
    }

    @Override
    public CompletionStage<String> getBlockHash(int height) {
        return new TryAllAvailableHosts<>((RpcClient client) -> client.getBlockHash(height)).get();
    }

    @Override
    public CompletionStage<Block> getBlock(String hash) {
        return new TryAllAvailableHosts<>((RpcClient client) -> client.getBlock(hash)).get();
    }

    @Override
    public CompletionStage<BlockHeader> getBlockHeader(String fromHash) {
        return new TryAllAvailableHosts<>((RpcClient client) -> client.getBlockHeader(fromHash)).get();
    }

    private synchronized RpcClientWithConnections takeAndIncrement() throws InterruptedException {
        RpcClientWithConnections take = queue.take();
        take.connections++;
        take.age = this.currentAge;
        this.currentAge++;
        queue.add(take);
        return take;
    }

    private synchronized void removeAndDecrement(RpcClientWithConnections take) {
        boolean removed = queue.remove(take);
        take.connections--;
        if (!removed) {
            return;//banned by other concurrent request.
        }
        queue.add(take);
    }

    private void scheduleBannedNodeCheck(RpcClientWithConnections take) {
        bannedNodeChecker.schedule(() -> {
            take.client.getBlockCount().whenCompleteAsync((result, error) -> {
                if (result != null) {
                    System.out.println("Unbanning " + take.client);
                    take.age = currentAge;
                    this.currentAge++;
                    queue.add(take);//Node back online. Make available for requests.
                } else {
                    scheduleBannedNodeCheck(take);
                }
            });
        }, 10, TimeUnit.SECONDS);
    }

    private synchronized void banClient(RpcClientWithConnections take) {
        boolean removed = queue.remove(take);
        take.connections--;
        if (!removed) {
            return;//already banned by other concurrent request
        }
        System.out.println("Banning " + take.client);
        scheduleBannedNodeCheck(take);
    }

    @Override
    public void close() {
        bannedNodeChecker.shutdownNow();
        for (RpcClient client : clients) {
            client.close();
        }
    }

    private class TryAllAvailableHosts<E> implements Supplier<CompletionStage<E>> {

        private Function<RpcClient, CompletionStage<E>> clientAction;

        public TryAllAvailableHosts(Function<RpcClient, CompletionStage<E>> clientAction) {
            this.clientAction = clientAction;
        }

        @Override
        public CompletionStage<E> get() {
            try {
                RpcClientWithConnections take = takeAndIncrement();
                CompletionStage<E> res = this.clientAction.apply(take.client);
                return res.handleAsync(new BiFunction<E, Throwable, E>() {
                    @Override
                    public E apply(E result, Throwable error) {
                        if (result == null) {
                            banClient(take);
                            System.out.println("client error " + error);
                            if (queue.isEmpty()) {
                                throw new RuntimeException("No host available", error);
                            }
                            return null;//try next host
                        } else {
                            removeAndDecrement(take);
                            return result;
                        }
                    }
                }).thenComposeAsync(new Function<E, CompletionStage<E>>() {
                    @Override
                    public CompletionStage<E> apply(E result) {
                        if (result == null) {
                            return get();
                        } else {
                            return CompletableFuture.completedFuture(result);
                        }
                    }
                });
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
        }

    }

    private class RpcClientWithConnections implements Comparable<RpcClientWithConnections>, Serializable {

        private final RpcClient client;
        private Integer connections;
        private Long age;

        public RpcClientWithConnections(RpcClient client, long age) {
            this.client = client;
            this.connections = 0;
            this.age = age;
        }

        @Override
        public int compareTo(RpcClientWithConnections o) {
            int res = this.connections.compareTo(o.connections);
            if (res != 0) {
                return res;
            }
            return this.age.compareTo(o.age);//break ties by choosing least recently used first
        }
    }

}
