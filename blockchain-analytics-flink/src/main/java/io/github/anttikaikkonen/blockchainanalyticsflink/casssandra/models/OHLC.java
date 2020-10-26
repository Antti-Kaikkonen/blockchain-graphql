package io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import java.util.Date;

@Table(name="ohlc")
public class OHLC {
    
    @PartitionKey(0)
    String address;

    Long open;
    Long high;
    Long low;
    Long close;
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

    public Long getOpen() {
        return open;
    }

    public void setOpen(Long open) {
        this.open = open;
    }

    public Long getHigh() {
        return high;
    }

    public void setHigh(Long high) {
        this.high = high;
    }

    public Long getLow() {
        return low;
    }

    public void setLow(Long low) {
        this.low = low;
    }

    public Long getClose() {
        return close;
    }

    public void setClose(Long close) {
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
        return address+" O "+open/1.0e8+"\tH "+high/1.0e8+"\tL "+low/1.0e8+"\tC "+close/1.0e8 + "\t" + timestamp;
    }
}
