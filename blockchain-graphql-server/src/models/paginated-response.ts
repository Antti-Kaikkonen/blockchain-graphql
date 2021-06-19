import { ObjectType, ClassType, Field } from 'type-graphql'

export function PaginatedResponse<TItem>(TItemClass: ClassType<TItem>): abstract new () => { items: TItem[], hasMore: boolean } {
    // `isAbstract` decorator option is mandatory to prevent registering in schema
    @ObjectType({ isAbstract: true })
    abstract class PaginatedResponseClass {
        // here we use the runtime argument
        @Field(type => [TItemClass], { nullable: false })
        // and here the generic type
        items: TItem[]

        //@Field({complexity: 0}) //Specifying complexity here does not work so we force to implement this field in the subclass where complexity works
        abstract hasMore: boolean
    }
    return PaginatedResponseClass
}