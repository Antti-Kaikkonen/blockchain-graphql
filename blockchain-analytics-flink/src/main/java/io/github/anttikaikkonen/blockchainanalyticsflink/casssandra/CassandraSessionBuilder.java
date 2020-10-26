package io.github.anttikaikkonen.blockchainanalyticsflink.casssandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import java.io.Serializable;

public abstract class CassandraSessionBuilder implements Serializable {
    
    protected abstract Session createSession(Cluster.Builder builder);
    
    public Session build() {
        return this.createSession(Cluster.builder());
    }
    
}
