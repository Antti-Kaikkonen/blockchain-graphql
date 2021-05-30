package io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import java.util.Date;

@Table(name = "ohlc")
public class OHLC {

    @PartitionKey(0)
    String address;

    Double open;
    Double high;
    Double low;
    Double close;
    @ClusteringColumn(0)
    Date timestamp;
    @PartitionKey(1)
    Integer interval;

    public OHLC() {

    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public Double getOpen() {
        return open;
    }

    public void setOpen(Double open) {
        this.open = open;
    }

    public Double getHigh() {
        return high;
    }

    public void setHigh(Double high) {
        this.high = high;
    }

    public Double getLow() {
        return low;
    }

    public void setLow(Double low) {
        this.low = low;
    }

    public Double getClose() {
        return close;
    }

    public void setClose(Double close) {
        this.close = close;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    public Integer getInterval() {
        return interval;
    }

    public void setInterval(Integer interval) {
        this.interval = interval;
    }

    @Override
    public String toString() {
        return address + " O " + open + "\tH " + high + "\tL " + low + "\tC " + close + "\t" + timestamp;
    }
}
