import http from "http";
import url, { URL } from "url";

class RpcResponse<T> {
    result: T;
    error: {message: string, code: number};
}

export class RpcMempool {
    [txid: string]: {
        size: number;
        fee: number;
        time: number;
        height: number;
        descendantcount: number;
        descendantsize: number;
        descendantfees: number;
        ancestorcount: number;
        ancestorsize: number;
        ancestorfees: number;
        depends: [];
    }
}

export class RpcScriptSig {
    asm: string;
    hex: string;
}

export class RpcScriptPubKey {
    asm: string;
    hex: string;
    reqSigs: number;
    type: string;
    addresses: string[];
}

export class RpcVin {
    coinbase: string;
    txid: string;
    vout: number;
    scriptSig: RpcScriptSig;
    sequence: number;
}

export class RpcVout {
    value: number;
    n: number;
    scriptPubKey: RpcScriptPubKey;
}

export class RpcTx {
    txid: string;
    version: number;
    type: number;
    size: number;
    locktime: number;
    vin: RpcVin[];
    vout: RpcVout[];
}

export class RpcBlockNoTx {
    public hash: string;
    public confirmations: number;
    public size: number;
    public height: number;
    public version: number;
    public versionHex: string;
    public merkleroot: string;
    public time: number;
    public mediantime: number;
    public nonce: number;
    public bits: string;
    public difficulty: number;
    public chainwork: string;
    public nTx: number;
    public previousblockhash: string;
    public nextblockhash: string;
}

export class RpcBlock extends RpcBlockNoTx{
    public tx: RpcTx[];
}


export class RpcClient {

    private rpc_url: url.UrlWithStringQuery;

    constructor(rpc_urls: string[], private rpc_username: string, private rpc_password: string) {
        this.rpc_url = url.parse(rpc_urls[0]);
    }
    
    public getBlockCount(): Promise<number> {
        return new Promise<number>((resolve, reject) => {
            let req = http.request({
                hostname: this.rpc_url.hostname,
                port: this.rpc_url.port,
                protocol: this.rpc_url.protocol,
                auth: this.rpc_username + ":" + this.rpc_password,
                method: "POST"
            }, (res: http.IncomingMessage) => {
                let result = "";
                res.on('data', (d) => {
                    result+=d;
                })
                res.on('end', () => {
                    let obj: RpcResponse<number> = JSON.parse(result);
                    if (obj.error) {
                        reject(obj.error.message);
                    } else {
                        resolve(obj.result);
                    }
                });
            });
    
            req.on('error', (error: Error) => {
                reject(error);
            });
            req.write(JSON.stringify({method:"getblockcount", params:[]}));
            req.end();
        });
    }

    public getBlockHash(height: number): Promise<string> {
        return new Promise<string>((resolve, reject) => {
            let req = http.request({
                hostname: this.rpc_url.hostname,
                port: this.rpc_url.port,
                protocol: this.rpc_url.protocol,
                auth: this.rpc_username + ":" + this.rpc_password,
                method: "POST"
            }, (res: http.IncomingMessage) => {
                let result = "";
                res.on('data', (d) => {
                    result+=d;
                })
                res.on('end', () => {
                    let obj: RpcResponse<string> = JSON.parse(result);
                    if (obj.error) {
                        reject(obj.error.message);
                    } else {
                        resolve(obj.result);
                    }
                });
            });
    
            req.on('error', (error: Error) => {
                reject(error);
            });
            req.write(JSON.stringify({method:"getblockhash", params:[height]}));
            req.end();
        });
    }

    public getBlock(hash: string): Promise<RpcBlock> {
        return new Promise<RpcBlock>((resolve, reject) => {
            let req = http.request({
                hostname: this.rpc_url.hostname,
                port: this.rpc_url.port,
                protocol: this.rpc_url.protocol,
                auth: this.rpc_username + ":" + this.rpc_password,
                method: "POST"
            }, (res: http.IncomingMessage) => {
                let result = "";
                res.on('data', (d) => {
                    result+=d;
                })
                res.on('end', () => {
                    let obj: RpcResponse<RpcBlock> = JSON.parse(result);
                    if (obj.error) {
                        reject(obj.error.message);
                    } else {
                        resolve(obj.result);
                    }
                });
            });
    
            req.on('error', (error: Error) => {
                reject(error);
            });
            req.write(JSON.stringify({method:"getblock", params:[hash, 2]}));
            req.end();
        });
    }

    public getRawTransaction(txid: string): Promise<RpcTx> {
        return new Promise<RpcTx>((resolve, reject) => {
            let req = http.request({
                hostname: this.rpc_url.hostname,
                port: this.rpc_url.port,
                protocol: this.rpc_url.protocol,
                auth: this.rpc_username + ":" + this.rpc_password,
                method: "POST"
            }, (res: http.IncomingMessage) => {
                let result = "";
                res.on('data', (d) => {
                    result+=d;
                })
                res.on('end', () => {
                    let obj: RpcResponse<RpcTx> = JSON.parse(result);
                    if (obj.error) {
                        reject(obj.error.message);
                    } else {
                        resolve(obj.result);
                    }
                });
            });
    
            req.on('error', (error: Error) => {
                reject(error);
            });
            req.write(JSON.stringify({method:"getrawtransaction", params:[txid, 2]}));
            req.end();
        });
    }

    public getRawMempool(): Promise<string[]> {
        return new Promise<string[]>((resolve, reject) => {
            let req = http.request({
                hostname: this.rpc_url.hostname,
                port: this.rpc_url.port,
                protocol: this.rpc_url.protocol,
                auth: this.rpc_username + ":" + this.rpc_password,
                method: "POST"
            }, (res: http.IncomingMessage) => {
                let result = "";
                res.on('data', (d) => {
                    result+=d;
                })
                res.on('end', () => {
                    let obj: RpcResponse<string[]> = JSON.parse(result);
                    if (obj.error) {
                        reject(obj.error.message);
                    } else {
                        resolve(obj.result);
                    }
                });
            });
    
            req.on('error', (error: Error) => {
                reject(error);
            });
            req.write(JSON.stringify({method:"getrawmempool", params:[]}));
            req.end();
        });
    }

    public getMempool(): Promise<RpcMempool> {
        return new Promise<RpcMempool>((resolve, reject) => {
            let req = http.request({
                hostname: this.rpc_url.hostname,
                port: this.rpc_url.port,
                protocol: this.rpc_url.protocol,
                auth: this.rpc_username + ":" + this.rpc_password,
                method: "POST"
            }, (res: http.IncomingMessage) => {
                let result = "";
                res.on('data', (d) => {
                    result+=d;
                })
                res.on('end', () => {
                    let obj: RpcResponse<RpcMempool> = JSON.parse(result);
                    if (obj.error) {
                        reject(obj.error.message);
                    } else {
                        resolve(obj.result);
                    }
                });
            });
    
            req.on('error', (error: Error) => {
                reject(error);
            });
            req.write(JSON.stringify({method:"getrawmempool", params:[true]}));
            req.end();
        });
    }

   
}