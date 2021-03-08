import { ObjectType, Field } from "type-graphql";
import { PaginatedAddressTransactionResponse } from "./address-transaction";
import { PaginatedOHLCResponse } from "./ohlc";
import { PaginatedAddressBalanceResponse } from "./address-balance";
import { AddressCluster } from "./address-cluster";
import { PaginatedResponse } from "./paginated-response";
import { Coin } from "./coin";

@ObjectType()
export class Address {

    constructor({ address, coin }: { address: string, coin: Coin }) {
        this.address = address;
        this.coin = coin;
    }

    readonly coin: Coin;

    @Field({ nullable: false, complexity: 1 })
    readonly address: string;

}

@ObjectType()
export class PaginatedAddressResponse extends PaginatedResponse(Address) {
    @Field({ nullable: false, complexity: 0 })
    hasMore: boolean;
}