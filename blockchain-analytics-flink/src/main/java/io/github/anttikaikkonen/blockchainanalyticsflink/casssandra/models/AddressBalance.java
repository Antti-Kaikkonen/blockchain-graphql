package io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import java.util.Date;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;



@Table(name = "address_balance")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AddressBalance {
    
    @PartitionKey
    private String address;
    
    @ClusteringColumn(0)
    private Date timestamp;
    
    private double balance;
    
}
