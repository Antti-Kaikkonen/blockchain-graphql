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
@Table(name = "transaction_input")
public class TransactionInput {

    @PartitionKey
    String spending_txid;
    @ClusteringColumn(0)
    int spending_index;

    String txid;
    int vout;
    String coinbase;
    long sequence;
    @Column(name = "scriptsig")
    ScriptSig scriptSig;

}
