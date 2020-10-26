package io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import io.github.anttikaikkonen.bitcoinrpcclientjava.models.BlockHeader;
import java.math.BigDecimal;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Table(name="block")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Block {
    
    public Block(io.github.anttikaikkonen.bitcoinrpcclientjava.models.Block block) {
        setBits(block.getBits());
        setChainwork(block.getChainwork());
        setDifficulty(block.getDifficulty());
        setHash(block.getHash());
        setHeight(block.getHeight());
        setMediantime(block.getMediantime());
        setMerkleroot(block.getMerkleroot());
        setNonce(block.getNonce());
        setPreviousblockhash(block.getPreviousblockhash());
        setTime(block.getTime());
        setVersion(block.getVersion());
        setVersionHex(block.getVersionHex());
        setSize(block.getSize());
        setTx_count(block.getTx().length);
    }
    
    public Block(BlockHeader header, int size, int tx_count) {
        setBits(header.getBits());
        setChainwork(header.getChainwork());
        setDifficulty(header.getDifficulty());
        setHash(header.getHash());
        setHeight(header.getHeight());
        setMediantime(header.getMediantime());
        setMerkleroot(header.getMerkleroot());
        setNonce(header.getNonce());
        setPreviousblockhash(header.getPreviousblockhash());
        setTime(header.getTime());
        setVersion(header.getVersion());
        setVersionHex(header.getVersionHex());
        setSize(size);
        setTx_count(tx_count);
    }
    
    @PartitionKey
    String hash;
    
    @Column(name="versionhex")
    String versionHex;
    
    int size;
    
    @Column(name="tx_count")
    int tx_count;
    
    
    int height;
    int version;
    String merkleroot;
    long time;
    long mediantime;
    long nonce;
    String bits;
    BigDecimal difficulty;
    String chainwork;
    String previousblockhash;
    
}
