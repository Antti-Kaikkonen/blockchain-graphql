package io.github.anttikaikkonen.blockchainanalyticsflink.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class InputPointer {

    String spending_txid;
    String txid;
    Integer vout;
    Integer input_index;
}
