import "reflect-metadata";
import { Client } from "cassandra-driver";
import { buildSchema, PubSubEngine } from "type-graphql";
import { RichlistResolver } from './resolvers/richlist-resolver';
import { AddressTransactionsResolver } from './resolvers/address-transactions-resolver';
import { ApolloServer, PubSub } from "apollo-server";
import { Container } from "typedi";
import { AddressResolver } from "./resolvers/address-resolver";
import { DateResolver } from "./resolvers/date-resolver";
import {
    getComplexity,
    simpleEstimator,
    fieldExtensionsEstimator
  } from 'graphql-query-complexity';
import { BlockResolver } from "./resolvers/block-resolver";
import { ConfirmedTransactionResolver } from "./resolvers/confirmed-transaction-resolver";
import { BlockHashResolver } from "./resolvers/block-hash-resolver";
import { TransactionResolver } from "./resolvers/transaction-resolver";
import { TransactionInputResolver } from "./resolvers/transaction-input-resolver";
import { TransactionOutputResolver } from "./resolvers/transaction-output-resolver";
import { AddressClusterResolver } from "./resolvers/address-cluster-resolver";
import { ClusterTransactionResolver } from "./resolvers/cluster-transaction-resolver";
import { config } from "dotenv";


async function run() {

    const result = config();
    if (result.error) {
      throw result.error;
    }

    let contactPoints = process.env.CASSANDRA_HOST;
    let contactPointsArr = contactPoints.split(/\s+/);

    let keyspace = process.env.CASSANDRA_KEYSPACE;

    const client = new Client({
      contactPoints: contactPointsArr,
      localDataCenter: 'datacenter1',
      keyspace: keyspace
    });
    await client.connect();
    Container.set("cassandra_client", client);
    let schema = await buildSchema({
        resolvers: [RichlistResolver, AddressTransactionsResolver, AddressResolver, 
          DateResolver, BlockResolver, ConfirmedTransactionResolver, 
          BlockHashResolver, TransactionResolver, TransactionInputResolver, 
          TransactionOutputResolver, AddressClusterResolver, ClusterTransactionResolver],
        validate: true,
        container: Container,
        dateScalarMode: "timestamp"
    });
    const server = new ApolloServer({
        schema,
        playground: true,
        debug: false,
        plugins: [
            {
              requestDidStart: () => ({
                didResolveOperation({ request, document }) {
                  /**
                   * This provides GraphQL query analysis to be able to react on complex queries to your GraphQL server.
                   * This can be used to protect your GraphQL servers against resource exhaustion and DoS attacks.
                   * More documentation can be found at https://github.com/ivome/graphql-query-complexity.
                   */
                  const complexity = getComplexity({
                    // Our built schema
                    schema,
                    // To calculate query complexity properly,
                    // we have to check only the requested operation
                    // not the whole document that may contains multiple operations
                    operationName: request.operationName,
                    // The GraphQL query document
                    query: document,
                    // The variables for our GraphQL query
                    variables: request.variables,
                    // Add any number of estimators. The estimators are invoked in order, the first
                    // numeric value that is being returned by an estimator is used as the field complexity.
                    // If no estimator returns a value, an exception is raised.
                    estimators: [
                      // Using fieldExtensionsEstimator is mandatory to make it work with type-graphql.
                      fieldExtensionsEstimator(),
                      // Add more estimators here...
                      // This will assign each field a complexity of 1
                      // if no other estimator returned a value.
                      simpleEstimator({ defaultComplexity: 1 }),
                    ],
                  });
                  // Here we can react to the calculated complexity,
                  // like compare it with max and throw error when the threshold is reached.
                  if (complexity >= 100000) {
                    throw new Error(
                      `Sorry, too complicated query! ${complexity} is over 100000 that is the max allowed complexity.`,
                    );
                  }
                  // And here we can e.g. subtract the complexity point from hourly API calls limit.
                  console.log("Used query complexity points:", complexity);
                },
              }),
            },
          ],
    });
    const { url } = await server.listen(6545);
    console.log(`Server is running, GraphQL Playground available at ${url}`);
}
  
run();