package io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@Table(name="transaction")
public class Transaction {
    @PartitionKey
    String txid;
    
    int height;
    
    @Column(name="tx_n")
    int txN;
    
    int size;
    long version;
    long locktime;
    
    int input_count;
    int output_count;
    long tx_fee;
}
