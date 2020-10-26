package io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models;

import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Table(name="longest_chain")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class LongestChain {
    
    @PartitionKey
    int height;
    
    String hash;
    
}