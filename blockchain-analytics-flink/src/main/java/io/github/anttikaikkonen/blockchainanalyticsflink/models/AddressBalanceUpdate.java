package io.github.anttikaikkonen.blockchainanalyticsflink.models;

import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.AddressBalance;
import java.util.Date;
import lombok.NoArgsConstructor;

@NoArgsConstructor
public class AddressBalanceUpdate extends AddressBalance {

    public AddressBalanceUpdate(String address, double balance, long previousBalance, Date timestamp) {
        this.setAddress(address);
        this.setBalance(balance);
        this.setTimestamp(timestamp);
        this.previousBalance = previousBalance;
    }

    private long previousBalance;

    public long getPreviousBalance() {
        return previousBalance;
    }

    public void setPreviousBalance(long previousBalance) {
        this.previousBalance = previousBalance;
    }

}
