package io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models;

import com.datastax.driver.core.LocalDate;
import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

@Table(name="daily_richlist")
public class RichList {

    public RichList() {
        
    }
    
    public RichList(String address, long balance, long previousBalance, long timestamp) {
        this.bin = (byte) address.hashCode();
        this.address = address;
        this.balance = balance;
        this.balanceChange = balance-previousBalance;
        this.date = LocalDate.fromDaysSinceEpoch((int) (timestamp/(1000*60*60*24)));
    }
    
    @PartitionKey(0)
    private LocalDate date;
    
    @PartitionKey(1)
    private byte bin;
    
    @ClusteringColumn(0)
    private long balance;
    @ClusteringColumn(1)
    @Column(name="balance_change")
    private long balanceChange;
    @ClusteringColumn(2)
    private String address;

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public long getBalance() {
        return balance;
    }

    public void setBalance(long balance) {
        this.balance = balance;
    }

    public long getBalanceChange() {
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
        return address+"\t"+balance+" ("+ (balanceChange > 0 ? "+":"") + balanceChange+")\t"+date;
    }
    
}
