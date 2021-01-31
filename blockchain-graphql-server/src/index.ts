import "reflect-metadata";
import { Client } from "cassandra-driver";
import { buildSchema } from "type-graphql";
import { RichlistResolver } from './resolvers/richlist-resolver';
import { AddressTransactionsResolver } from './resolvers/address-transactions-resolver';
import { ApolloServer } from "apollo-server";
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
import { CoinResolver } from "./resolvers/coin-resolver";
import { Coin } from "./models/coin";
import { CoinsUpdater } from "./coins-updater";
import { LimitedCapacityClient } from "./limited-capacity-client";


async function run() {

    const result = config();
    if (result.error) {
      throw result.error;
    }

    let contactPoints = process.env.CASSANDRA_HOST;
    let contactPointsArr = contactPoints.split(/\s+/);

    let coins_keyspace = process.env.CASSANDRA_COINS_KEYSPACE || "coins";

    let api_port: number = process.env.API_PORT !== undefined ? Number.parseInt(process.env.API_PORT) : 6545;
    
    const client = new Client({
      contactPoints: contactPointsArr,
      localDataCenter: 'datacenter1',
      
    });
    await client.connect();

    const limitedCapcityClient = new LimitedCapacityClient(client, 100);

    let nameToCoin: Map<string, Coin> = new Map();
    let coins_updater: CoinsUpdater = new CoinsUpdater(nameToCoin, limitedCapcityClient, coins_keyspace);
    await coins_updater.start();
    Container.set("coins", nameToCoin);
    Container.set("cassandra_client", limitedCapcityClient);
    Container.set("coins_keyspace", coins_keyspace);

    let schema = await buildSchema({
        resolvers: [RichlistResolver, AddressTransactionsResolver, AddressResolver, 
          DateResolver, BlockResolver, ConfirmedTransactionResolver, 
          BlockHashResolver, TransactionResolver, TransactionInputResolver, 
          TransactionOutputResolver, AddressClusterResolver, ClusterTransactionResolver, CoinResolver],
        validate: true,
        container: Container,
        dateScalarMode: "timestamp"
    });

    const ipToQueries: Map<string, number> = new Map();
    let clearTime = new Date().getTime() + 60000;
    setInterval(() => {
      ipToQueries.clear();
      clearTime = new Date().getTime() + 60000;
    }, 60000);

    const server = new ApolloServer({
        schema,
        playground: true,
        debug: false,
        plugins: [
            {
              requestDidStart: () => ({
                didResolveOperation(requestContext) {
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
                    operationName: requestContext.request.operationName,
                    // The GraphQL query document
                    query: requestContext.document,
                    // The variables for our GraphQL query
                    variables: requestContext.request.variables,
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
                      `Sorry, too complicated query! ${complexity} is over 100000 that is the max allowed complexity.`
                    );
                  }
                  // And here we can e.g. subtract the complexity point from hourly API calls limit.
                  console.log("Used query complexity points:", complexity);
                  if (complexity > 0) {
                    //Your HTTP server (e.g NGINX or Apache) must send client IP-address with this header for rate limiting to take effect. 
                    let x_forwarded_for: string = requestContext.request.http.headers.get('X-Forwarded-For');
                    if (x_forwarded_for !== undefined && x_forwarded_for !== null) {
                      let ips: string[] = x_forwarded_for.split(", ");
                      if (ips.length > 0) {
                        const ip: string = ips[ips.length-1];
                        const oldValue = ipToQueries.get(ip);
                        const newValue = oldValue === undefined ? 1 : oldValue + 1;
                        ipToQueries.set(ip, newValue);
                        if (newValue > 600) {
                          const secondsLeft = Math.ceil( (clearTime-new Date().getTime()) / 1000);
                          throw new Error(
                            `Sorry, too many queries from your IP address. Try again in ${secondsLeft} seconds`,
                          );
                        }
                      }
                    }
                  }
                },
              }),
            },
          ],
    });
    const { url } = await server.listen(api_port);
    console.log(`Server is running, GraphQL Playground available at ${url}`);
}
  
run();