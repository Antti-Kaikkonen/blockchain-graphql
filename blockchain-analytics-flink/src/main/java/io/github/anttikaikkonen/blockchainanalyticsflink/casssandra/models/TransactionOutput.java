package io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@Table(name = "transaction_output")
public class TransactionOutput {

    @PartitionKey
    String txid;
    @ClusteringColumn(0)
    int n;
    Double value;
    //@CqlName("scriptpubkey")
    @Column(name = "scriptpubkey")
    ScriptPubKey scriptPubKey;
    //Extra input properties
    String spending_txid;
    Integer spending_index;

}
