package io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models;

import com.datastax.driver.core.LocalDate;
import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Table(name="daily_top_gainers")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TopGainers {
    @PartitionKey
    private LocalDate date;
   
    @ClusteringColumn(1)
    private String address;
    
    @ClusteringColumn(0)
    @Column(name="balance_change")
    private long balanceChange;
    
}
