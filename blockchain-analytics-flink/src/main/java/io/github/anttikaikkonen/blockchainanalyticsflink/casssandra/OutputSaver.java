package io.github.anttikaikkonen.blockchainanalyticsflink.casssandra;

import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.google.common.util.concurrent.ListenableFuture;
import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.ScriptPubKey;
import io.github.anttikaikkonen.bitcoinrpcclientjava.models.TransactionOutput;
import org.apache.flink.api.java.tuple.Tuple2;


public class OutputSaver extends CassandraSaverFunction<Tuple2<TransactionOutput, String>> {

    private Mapper<io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.TransactionOutput> mapper;
    
    public OutputSaver(CassandraSessionBuilder sessionBuilder) {
        super(sessionBuilder);
    }

    @Override
    public ListenableFuture saveAsync(Tuple2<TransactionOutput, String> input) {
        io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.TransactionOutput vout2 = new io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.TransactionOutput();
        vout2.setTxid(input.f1);
        vout2.setN(input.f0.getN());
        ScriptPubKey scriptpubKey = new ScriptPubKey(input.f0.getScriptPubKey());
        vout2.setScriptPubKey(scriptpubKey);
        vout2.setValue(input.f0.getValue());
        
        return mapper.saveAsync(vout2);
    }

    @Override
    public void initMappers(MappingManager manager) {
        this.mapper = manager.mapper(io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.TransactionOutput.class);
    }
    
}
