package io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Table(name="confirmed_transaction")
public class ConfirmedTransaction {
    
    @PartitionKey
    int height;
    
    //@CqlName("tx_n")
    @ClusteringColumn(0)
    @Column(name="tx_n")
    int txN;
    
    String txid;
}
