package io.github.anttikaikkonen.blockchainanalyticsflink.casssandra;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.google.common.util.concurrent.ListenableFuture;
import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.AddressBalance;
import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.OHLC;
import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.RichList;
import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.TopGainers;
import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.TopLosers;
import io.github.anttikaikkonen.blockchainanalyticsflink.statefun.cassandraexecutor.AddressOperation;
import io.github.anttikaikkonen.blockchainanalyticsflink.statefun.cassandraexecutor.DeleteAddresses;
import io.github.anttikaikkonen.blockchainanalyticsflink.statefun.cassandraexecutor.DeleteTransactions;
import io.github.anttikaikkonen.blockchainanalyticsflink.statefun.cassandraexecutor.SetParent;
import io.github.anttikaikkonen.blockchainanalyticsflink.statefun.unionfind.AddAddressOperation;
import io.github.anttikaikkonen.blockchainanalyticsflink.statefun.unionfind.AddTransactionOperation;
import java.time.Instant;
import java.util.Date;

public class AddressSink extends CassandraSaverFunction<Object> {
    private Mapper<OHLC> ohlcMapper;
    private Mapper<TopGainers> gainersMapper;
    private Mapper<TopLosers> losersMapper;
    private Mapper<RichList> richListMapper;
    private Mapper<AddressBalance> addressBalanceMapper;
    
    private transient PreparedStatement addTransactionStatement;
    
    private transient PreparedStatement setParentStatement;
    
    private transient PreparedStatement makesetStatement;
    
    private transient PreparedStatement addAddressStatement;
    
    private transient PreparedStatement deleteAddressesStatement;
    
    private transient PreparedStatement deleteTransactionsStatement;
    
    private transient Session session;
    
    public AddressSink(CassandraSessionBuilder sessionBuilder) {
        super(sessionBuilder);
    }
    
    @Override
    public ListenableFuture saveAsync(Object input) {
        if (input instanceof AddressOperation) {
            AddressOperation addressOp = (AddressOperation) input;
            String address = addressOp.getAddress();
            Object op = addressOp.getOp();
            if (op instanceof SetParent) {
                SetParent setParent = (SetParent) op;
                if (setParent.getParent() == null) {
                    return this.session.executeAsync(this.makesetStatement.bind(address));
                } else {
                    return this.session.executeAsync(this.setParentStatement.bind(address, setParent.getParent()));
                }
            } else if (op instanceof AddAddressOperation) {
                AddAddressOperation addAddress = (AddAddressOperation) op;
                return this.session.executeAsync(this.addAddressStatement.bind(address, addAddress.getAddress()));
            } else if (op instanceof AddTransactionOperation) {
                AddTransactionOperation addTransaction = (AddTransactionOperation) op;
                return this.session.executeAsync(this.addTransactionStatement.bind(address, Date.from(Instant.ofEpochMilli(addTransaction.getTime())), addTransaction.getHeight(), addTransaction.getTx_n(), addTransaction.getDelta()));
            } else if (op instanceof DeleteAddresses) {
                return this.session.executeAsync(this.deleteAddressesStatement.bind(address));
            } else if (op instanceof DeleteTransactions) {
                return this.session.executeAsync(this.deleteTransactionsStatement.bind(address));
            } else {
                op.getClass();
                throw new RuntimeException("Unsupported AddressOperation type");
            }
        } else if (input instanceof OHLC) {
            return this.ohlcMapper.saveAsync((OHLC) input);
        } else if (input instanceof TopGainers) {
            return this.gainersMapper.saveAsync((TopGainers) input);
        } else if (input instanceof TopLosers) {
            return this.losersMapper.saveAsync((TopLosers) input);
        } else if (input instanceof RichList) {
            return this.richListMapper.saveAsync((RichList) input);
        } else if (input instanceof AddressBalance) {
            return this.addressBalanceMapper.saveAsync((AddressBalance) input);
        } else {
            throw new RuntimeException("Unsupported type");
        }
    }

    @Override
    public void initMappers(MappingManager manager) {
        session = manager.getSession();
        this.ohlcMapper = manager.mapper(OHLC.class);
        this.gainersMapper = manager.mapper(TopGainers.class);
        this.losersMapper = manager.mapper(TopLosers.class);
        this.richListMapper = manager.mapper(RichList.class);
        this.addressBalanceMapper = manager.mapper(AddressBalance.class);
        addTransactionStatement = session.prepare("INSERT INTO cluster_transaction (cluster_id, timestamp, height, tx_n, balance_change) VALUES (?, ?, ?, ?, ?)");
        setParentStatement = session.prepare("INSERT INTO union_find (address, parent) VALUES (?, ?)");
        makesetStatement = session.prepare("INSERT INTO union_find (address) VALUES (?)");
        addAddressStatement = session.prepare("INSERT INTO cluster_address (cluster_id, address) VALUES (?, ?)");

        deleteAddressesStatement = session.prepare("DELETE FROM cluster_address WHERE cluster_id = ?");
        deleteTransactionsStatement = session.prepare("DELETE FROM cluster_transaction WHERE cluster_id = ?");
    }



}
