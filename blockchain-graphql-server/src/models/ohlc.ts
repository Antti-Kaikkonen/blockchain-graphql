import { ObjectType, InputType, Field, Int } from "type-graphql";
import { PaginatedResponse } from "./paginated-response";

@ObjectType()
export class OHLC {

  @Field({complexity: 1})
  open!: number;

  @Field({complexity: 1})
  high!: number;

  @Field({complexity: 1})
  low!: number;

  @Field({complexity: 1})
  close!: number;

  @Field({complexity: 1})
  timestamp!: Date;

}

@InputType()
export class OHLCCursor {

  @Field({nullable: false})
  timestamp: Date;

  public static max(): OHLCCursor {
    let res = new OHLCCursor();
    res.timestamp = new Date(0);
    return res;
  }

}

@ObjectType()
export class PaginatedOHLCResponse extends PaginatedResponse(OHLC) {
}