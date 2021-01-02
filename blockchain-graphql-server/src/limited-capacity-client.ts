import { ArrayOrObject, Client, QueryOptions, types } from "cassandra-driver";
import { Semaphore } from "./semaphore";

export class LimitedCapacityClient {

    private sem: Semaphore;

    constructor(private client: Client, concurrency: number) {
        this.sem = new Semaphore(concurrency);
    }

    public async execute(query: string, params?: ArrayOrObject, options?: QueryOptions): Promise<types.ResultSet> {
        await this.sem.acquire();
        let p = this.client.execute(query, params, options);
        p.finally(() => {
            this.sem.release();
        })
        return p;
    }
}