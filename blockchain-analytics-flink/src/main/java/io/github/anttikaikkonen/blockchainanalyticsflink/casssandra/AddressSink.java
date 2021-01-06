package io.github.anttikaikkonen.blockchainanalyticsflink.casssandra;

import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.AddressBalance;
import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.OHLC;
import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.RichList;
import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.TopGainers;
import io.github.anttikaikkonen.blockchainanalyticsflink.casssandra.models.TopLosers;
import io.github.anttikaikkonen.blockchainanalyticsflink.statefun.cassandraexecutor.AddressOperation;
import io.github.anttikaikkonen.blockchainanalyticsflink.statefun.cassandraexecutor.DeleteCluster;
import io.github.anttikaikkonen.blockchainanalyticsflink.statefun.cassandraexecutor.SetBalanceOperation;
import io.github.anttikaikkonen.blockchainanalyticsflink.statefun.cassandraexecutor.SetDailyBalanceChange;
import io.github.anttikaikkonen.blockchainanalyticsflink.statefun.cassandraexecutor.SetParent;
import io.github.anttikaikkonen.blockchainanalyticsflink.statefun.unionfind.AddAddressOperation;
import io.github.anttikaikkonen.blockchainanalyticsflink.statefun.unionfind.AddTransactionOperation;
import java.time.Instant;
import java.util.Date;

public class AddressSink extends CassandraSaverFunction<Object> {
    
    public static final int BIN_COUNT = 10;
    
    private transient Mapper<OHLC> ohlcMapper;
    private transient Mapper<TopGainers> gainersMapper;
    private transient Mapper<TopLosers> losersMapper;
    private transient Mapper<RichList> richListMapper;
    private transient Mapper<AddressBalance> addressBalanceMapper;
    
    private transient PreparedStatement addTransactionStatement;
    
    private transient PreparedStatement setParentStatement;
    
    private transient PreparedStatement makesetStatement;
    
    private transient PreparedStatement addAddressStatement;
    
    private transient PreparedStatement deleteAddressesStatement;
    
    private transient PreparedStatement deleteTransactionsStatement;
    
    private transient PreparedStatement setBalanceStatement;
    
    private transient PreparedStatement deleteBalanceStatement;
    
    private transient PreparedStatement deleteDailyBalanceChangesStatement;
    
    private transient PreparedStatement setDailyBalanceChangeStatement;
    
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
            byte bin = (byte) (Math.abs(address.hashCode())%BIN_COUNT);
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
                return this.session.executeAsync(this.addTransactionStatement.bind(address, Date.from(Instant.ofEpochMilli(addTransaction.getTime())), addTransaction.getHeight(), addTransaction.getTx_n(), (double) addTransaction.getDelta()/1e8));
            } else if (op instanceof SetDailyBalanceChange) {
                SetDailyBalanceChange setDailyBalanceChange = (SetDailyBalanceChange) op;
                return this.session.executeAsync(this.setDailyBalanceChangeStatement.bind(address, bin, LocalDate.fromDaysSinceEpoch((int)setDailyBalanceChange.getEpochDate()), (double) setDailyBalanceChange.getBalanceChange()/1e8));
            } else if (op instanceof DeleteCluster) {
                return Futures.allAsList(
                        this.session.executeAsync(this.deleteAddressesStatement.bind(address)),
                        this.session.executeAsync(this.deleteTransactionsStatement.bind(address)),
                        this.session.executeAsync(this.deleteDailyBalanceChangesStatement.bind(address, bin)),
                        this.session.executeAsync(this.deleteBalanceStatement.bind(address, bin))
                );
            } else if (op instanceof SetBalanceOperation) {
                SetBalanceOperation setBalance = (SetBalanceOperation) op;
                return this.session.executeAsync(setBalanceStatement.bind(address, bin, (double)setBalance.getBalance()/1e8));
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
        
        setBalanceStatement = session.prepare("INSERT INTO cluster_details (cluster_id, bin, balance) VALUES (?, ?, ?)");
        setDailyBalanceChangeStatement = session.prepare("INSERT INTO cluster_daily_balance_change (cluster_id, bin, date, balance_change) VALUES (?, ?, ?, ?)");
        deleteBalanceStatement = session.prepare("DELETE FROM cluster_details WHERE cluster_id = ? AND bin = ?");
        deleteDailyBalanceChangesStatement = session.prepare("DELETE FROM cluster_daily_balance_change WHERE cluster_id = ? AND bin = ?");
    }



}
