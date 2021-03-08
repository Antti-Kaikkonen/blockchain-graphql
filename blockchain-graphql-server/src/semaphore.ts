import { Queue } from "./queue";

export class Semaphore {

    readonly initial_slots: number;

    constructor(public available_slots: number) {
        this.initial_slots = available_slots;
    }

    public promiseQueue: Queue<Promise<void>> = new Queue();
    public releaseQueue: Queue<() => void> = new Queue();
    public drainPromise: Promise<void>;
    //private queue: Array<()=>void> = [];


    public async acquire(): Promise<void> {
        if (this.available_slots > 0) {
            this.available_slots--;
        } else {
            this.available_slots--;
            const p: Promise<void> = new Promise<void>((resolve, reject) => {
                this.releaseQueue.enqueue(resolve);
            });
            this.promiseQueue.enqueue(p);
            await this.promiseQueue.dequeue();
        }
    }

    public release(): void {
        this.available_slots++;
        const releaseFunction = this.releaseQueue.dequeue();
        if (releaseFunction !== undefined) releaseFunction();
    }

    /*public async drain(): Promise<void> {
        if (this.promiseQueue.size() > 0) return;

    }*/

}