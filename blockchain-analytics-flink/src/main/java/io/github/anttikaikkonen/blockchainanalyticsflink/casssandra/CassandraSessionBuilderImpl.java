package io.github.anttikaikkonen.blockchainanalyticsflink.casssandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.WriteType;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.policies.RetryPolicy;
import io.github.anttikaikkonen.blockchainanalyticsflink.Main;
import java.io.Serializable;
import java.util.logging.Level;
import java.util.logging.Logger;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class CassandraSessionBuilderImpl extends CassandraSessionBuilder implements Serializable {

    private final String[] cassandraHosts;
    private final String cassandraKeyspace;

    @Override
    protected Session createSession(Cluster.Builder builder) {
        Cluster cluster = builder
                .addContactPoints(cassandraHosts)
                .withSocketOptions(
                        new SocketOptions().setReadTimeoutMillis(120000)
                )
                .withPoolingOptions(
                        new PoolingOptions()
                                .setConnectionsPerHost(HostDistance.LOCAL, 1, 1)
                                .setConnectionsPerHost(HostDistance.REMOTE, 1, 1)
                                .setMaxRequestsPerConnection(HostDistance.LOCAL, 30000)
                                .setMaxRequestsPerConnection(HostDistance.REMOTE, 30000)
                                .setMaxQueueSize(0)
                ).withRetryPolicy(new RetryPolicy() {
                    @Override
                    public RetryPolicy.RetryDecision onReadTimeout(Statement statement, ConsistencyLevel cl, int requiredResponses, int receivedResponses, boolean dataRetrieved, int nbRetry) {
                        System.out.println("onReadTimeout " + nbRetry);
                        if (nbRetry < 5) {
                            try {
                                Thread.sleep((nbRetry + 1) * 1000);
                            } catch (InterruptedException ex) {
                                Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
                            }
                            return RetryPolicy.RetryDecision.retry(cl);
                        } else {
                            return RetryPolicy.RetryDecision.rethrow();
                        }
                    }

                    @Override
                    public RetryPolicy.RetryDecision onWriteTimeout(Statement statement, ConsistencyLevel cl, WriteType arg2, int requiredAcks, int receivedAcks, int nbRetry) {
                        System.out.println("onWriteTimeout " + nbRetry);
                        if (nbRetry < 5) {
                            try {
                                Thread.sleep((nbRetry + 1) * 1000);
                            } catch (InterruptedException ex) {
                                Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
                            }
                            return RetryPolicy.RetryDecision.retry(cl);
                        } else {
                            return RetryPolicy.RetryDecision.rethrow();
                        }
                    }

                    @Override
                    public RetryPolicy.RetryDecision onUnavailable(Statement statement, ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry) {
                        System.out.println("onUnavailable " + nbRetry);
                        if (nbRetry < 5) {
                            try {
                                Thread.sleep((nbRetry + 1) * 1000);
                            } catch (InterruptedException ex) {
                                Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
                            }
                            return RetryPolicy.RetryDecision.retry(cl);
                        } else {
                            return RetryPolicy.RetryDecision.rethrow();
                        }
                    }

                    @Override
                    public RetryPolicy.RetryDecision onRequestError(Statement statement, ConsistencyLevel cl, DriverException driverException, int nbRetry) {
                        System.out.println("onRequestError " + nbRetry + ", ex:" + driverException);
                        if (nbRetry < 5) {
                            try {
                                Thread.sleep((nbRetry + 1) * 1000);
                            } catch (InterruptedException ex) {
                                Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
                            }
                            return RetryPolicy.RetryDecision.retry(cl);
                        } else {
                            return RetryPolicy.RetryDecision.rethrow();
                        }
                    }

                    @Override
                    public void init(Cluster arg0) {
                    }

                    @Override
                    public void close() {
                    }
                }).build();
        Session session = cluster.connect(cassandraKeyspace);
        return session;
    }

}
