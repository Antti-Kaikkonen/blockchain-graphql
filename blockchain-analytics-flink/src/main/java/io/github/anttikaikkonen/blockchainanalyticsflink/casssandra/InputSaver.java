package io.github.anttikaikkonen.blockchainanalyticsflink.casssandra;

import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.ScriptSig;
import io.github.anttikaikkonen.bitcoinrpcclientjava.models.TransactionInput;
import java.util.ArrayList;
import org.apache.flink.api.java.tuple.Tuple3;


public class InputSaver extends CassandraSaverFunction<Tuple3<TransactionInput, String, Integer>> {

    Mapper<io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.TransactionOutput> voutMapper;
    Mapper<io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.TransactionInput> vinMapper;
    
    public InputSaver(CassandraSessionBuilder sessionBuilder) {
        super(sessionBuilder);
    }
    

    @Override
    public ListenableFuture saveAsync(Tuple3<TransactionInput, String, Integer> input) {
        ArrayList<ListenableFuture<Void>> futures = new ArrayList<>();
        if (input.f0.getTxid() != null) {
            io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.TransactionOutput vout = new io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.TransactionOutput();
            vout.setTxid(input.f0.getTxid());
            vout.setN(input.f0.getVout());
            vout.setSpending_txid(input.f1);
            vout.setSpending_index(input.f2);
            futures.add(voutMapper.saveAsync(vout, Mapper.Option.saveNullFields(false)));//Update query
        }
        io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.TransactionInput vin2 = new io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.TransactionInput();
        vin2.setSpending_txid(input.f1);
        vin2.setSpending_index(input.f2);
        vin2.setCoinbase(input.f0.getCoinbase());
        if (input.f0.getScriptSig() != null) {
            ScriptSig scriptSig = new ScriptSig(input.f0.getScriptSig());
            vin2.setScriptSig(scriptSig);
        }
        vin2.setSequence(input.f0.getSequence());
        vin2.setTxid(input.f0.getTxid());
        vin2.setVout(input.f0.getVout());
        futures.add(vinMapper.saveAsync(vin2));
        return Futures.allAsList(futures);
    }

    @Override
    public void initMappers(MappingManager manager) {
        this.voutMapper = manager.mapper(io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.TransactionOutput.class);
        this.vinMapper = manager.mapper(io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.TransactionInput.class);
    }
    
}
