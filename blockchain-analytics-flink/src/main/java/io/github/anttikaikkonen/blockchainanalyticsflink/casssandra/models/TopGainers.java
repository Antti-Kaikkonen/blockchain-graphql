package io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models;

import com.datastax.driver.core.LocalDate;
import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Table(name = "daily_top_gainers")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TopGainers {

    public static final int BIN_COUNT = 20;

    public TopGainers(LocalDate date, String address, double balanceChange) {
        this.bin = (byte) (Math.abs(address.hashCode()) % BIN_COUNT);
        this.date = date;
        this.address = address;
        this.balanceChange = balanceChange;
    }

    @PartitionKey(0)
    private LocalDate date;

    @PartitionKey(1)
    private byte bin;

    @ClusteringColumn(1)
    private String address;

    @ClusteringColumn(0)
    @Column(name = "balance_change")
    private double balanceChange;

}
