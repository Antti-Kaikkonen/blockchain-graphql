export class Queue<E> {
    s1: E[] = []
    s2: E[] = []

    public enqueue(val: E): void {
        this.s1.push(val)
    }

    public dequeue(): E {
        if (this.s2.length === 0) {
            this.move()
        }
        return this.s2.pop() // return undefined if empty
    }

    public size(): number {
        return this.s1.length + this.s2.length
    }

    private move(): void {
        while (this.s1.length) {
            this.s2.push(this.s1.pop())
        }
    }
}