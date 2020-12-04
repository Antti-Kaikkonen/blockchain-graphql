import { ObjectType, Field, InputType } from "type-graphql";
import { Address } from "./address";
import { PaginatedResponse } from "./paginated-response";

@ObjectType()
export class Richlist {

  @Field({complexity: 1})
  address: Address;

  @Field({complexity: 1})
  balance: number;

  @Field({complexity: 1})
  balance_change: number;

}

@InputType()
export class RichListCursor {

  @Field({nullable: true, defaultValue: ""})
  address: string;

  @Field({nullable: false})
  balance: number;

  @Field({nullable: true, defaultValue: 0})
  balance_change: number;

  public static max(): RichListCursor {
    let res = new RichListCursor();
    res.balance = Number.MAX_VALUE;
    res.balance_change = Number.MAX_VALUE;
    res.address = "";
    return res;
  }

}

@ObjectType()
export class PaginatedRichlistResponse extends PaginatedResponse(Richlist) {
  @Field({complexity: 0})
  hasMore: boolean;
}