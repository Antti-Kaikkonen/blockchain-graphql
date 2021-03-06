package io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models;

import com.datastax.driver.core.LocalDate;
import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

@Table(name = "daily_richlist")
public class RichList {

    public static final int BIN_COUNT = 20;

    public RichList() {
    }

    public RichList(String address, double balance, double previousBalance, long timestamp) {
        this.bin = (byte) (Math.abs(address.hashCode()) % BIN_COUNT);
        this.address = address;
        this.balance = balance;
        long balanceChangeSats = Math.round(balance * 1e8) - Math.round(previousBalance * 1e8);
        this.balanceChange = (double) balanceChangeSats / 1e8;
        this.date = LocalDate.fromDaysSinceEpoch((int) (timestamp / (1000 * 60 * 60 * 24)));
    }

    @PartitionKey(0)
    private LocalDate date;

    @PartitionKey(1)
    private byte bin;

    @ClusteringColumn(0)
    private double balance;

    @ClusteringColumn(1)

    @Column(name = "balance_change")
    private double balanceChange;

    @ClusteringColumn(2)
    private String address;

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public double getBalance() {
        return balance;
    }

    public void setBalance(long balance) {
        this.balance = balance;
    }

    public double getBalanceChange() {
        return balanceChange;
    }

    public void setBalanceChange(long balanceChange) {
        this.balanceChange = balanceChange;
    }

    public LocalDate getDate() {
        return date;
    }

    public void setDate(LocalDate date) {
        this.date = date;
    }

    @Override
    public String toString() {
        return address + "\t" + balance + " (" + (balanceChange > 0 ? "+" : "") + balanceChange + ")\t" + date;
    }

}
