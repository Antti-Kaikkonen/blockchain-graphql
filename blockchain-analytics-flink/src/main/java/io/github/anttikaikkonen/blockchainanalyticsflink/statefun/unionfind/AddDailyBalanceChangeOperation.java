package io.github.anttikaikkonen.blockchainanalyticsflink.statefun.unionfind;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class AddDailyBalanceChangeOperation {
    private int epochDate;
    private long balanceChange;
}
