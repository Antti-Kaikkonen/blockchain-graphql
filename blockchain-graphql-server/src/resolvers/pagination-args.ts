import { ArgsType, Field, Int } from "type-graphql";
import { Max, Min } from "class-validator";

@ArgsType()
export class PaginationArgs {
  @Max(1000)
  @Min(1)
  @Field(type => Int)
  limit: number = 100;
}