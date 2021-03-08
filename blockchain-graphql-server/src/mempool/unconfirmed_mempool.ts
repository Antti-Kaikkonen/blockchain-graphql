import { TxDetails } from "./block-handler";
import { MempoolTx } from "./mempool";
import { RBTree } from 'bintrees';

interface TX {
    timestamp: number;
    txid: string;
    balanceChange: number;
}

export class AddressUnconfirmedMempool {

    transactions: RBTree<TX> = new RBTree((a, b) => {
        if (a.timestamp === b.timestamp) {
            if (a.txid === b.txid) {
                return 0;
            } else if (a.txid < b.txid) {
                return -1;
            } else {
                return 1;
            }
        } else if (a.timestamp < b.timestamp) {
            return -1;
        } else {
            return 1;
        }
    });

    public balanceChangeSat: number = 0;

    public add(tx: TX): void {
        this.balanceChangeSat += Math.round(tx.balanceChange * 1e8);
        this.transactions.insert(tx);
    }

    public remove(tx: TX): void {
        this.balanceChangeSat -= Math.round(tx.balanceChange * 1e8);
        this.transactions.remove(tx);
    }

}

export class UnconfirmedMempool {

    public addressMempools: Map<string, AddressUnconfirmedMempool> = new Map();

    public txids: RBTree<{ timestamp: number, txid: string }> = new RBTree((a, b) => {
        if (a.timestamp === b.timestamp) {
            if (a.txid === b.txid) {
                return 0;
            } else if (a.txid < b.txid) {
                return -1;
            } else {
                return 1;
            }
        } else if (a.timestamp < b.timestamp) {
            return -1;
        } else {
            return 1;
        }
    });

    public txs: Map<string, { tx: MempoolTx, txDetails: TxDetails, timestamp: number }> = new Map();

    public totalFeesSat: number = 0;

    public add(tx: MempoolTx, txDetails: TxDetails) {
        let time = new Date().getTime();
        this.txids.insert({ timestamp: time, txid: tx.rpcTx.txid });
        this.txs = this.txs.set(tx.rpcTx.txid, { tx: tx, txDetails: txDetails, timestamp: time });
        this.totalFeesSat += Math.round(txDetails.fee * 1e8);
        txDetails.addressDeltas.forEach((delta: number, address: string) => {
            let addressMempool: AddressUnconfirmedMempool = this.addressMempools.get(address);
            if (addressMempool === undefined) {
                addressMempool = new AddressUnconfirmedMempool();
                this.addressMempools.set(address, addressMempool);
            }
            addressMempool.add({ txid: tx.rpcTx.txid, timestamp: time, balanceChange: delta / 1e8 });
        });
    }

    public remove(txid: string) {//To remove a confirmed transaction or a double spent transaction
        let e = this.txs.get(txid);
        if (e !== undefined) {
            this.txids.remove({ timestamp: e.timestamp, txid: e.tx.rpcTx.txid });
            this.txs.delete(txid);
            this.totalFeesSat -= Math.round(e.txDetails.fee * 1e8);
            e.txDetails.addressDeltas.forEach((delta: number, address: string) => {
                if (this.addressMempools.get(address).transactions.size === 1) {
                    this.addressMempools.delete(address);
                } else {
                    this.addressMempools.get(address).remove({ timestamp: e.timestamp, txid: e.tx.rpcTx.txid, balanceChange: delta / 1e8 })
                }
            });
        }
    }


}