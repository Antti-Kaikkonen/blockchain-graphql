package io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import java.util.Date;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Table(name = "address_transaction")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AddressTransaction {

    @PartitionKey
    private String address;

    @ClusteringColumn(0)
    private Date timestamp;

    @ClusteringColumn(1)
    private int height;

    @ClusteringColumn(2)
    //@CqlName("tx_n")
    @Column(name = "tx_n")
    private int txN;

    private double balance_change;

    //private String txid;
}
