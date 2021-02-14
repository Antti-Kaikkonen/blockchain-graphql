# Using BlockchainGraphQL API inside a React App

We create a simple Reat app with TypeScript and demonstrate how to generate strongly typed functions from BlockchainGraphQL queries.

1. `yarn create react-app react-blockchain-app --template typescript` ([more info](https://create-react-app.dev/docs/adding-typescript/))
2. `cd react-blockchain-app`
3. `yarn add graphql @apollo/client`
4. `yarn add -D @graphql-codegen/cli` ([more info](https://graphql-code-generator.com/docs/getting-started/index))
5. `yarn graphql-codegen init`
    1. **Q:** What type of application are you building? **A:** `Application built with React`
    2. **Q:** Where is your schema? **A:** `https://blockchaingraphql.com`
    3. **Q:** Where are your operations and fragments? **A:** `src/**/*.graphql` (default)
    4. **Q:** Pick plugins **A:** `TypeScript`, `TypeScript Operations` and `TypeScript React Apollo`
    5. **Q:** Where to write the output **A:** `src/generated/graphql.tsx` (default)
    6. **Q:** Do you want to generate an introspection file? **A:** `n`
    7. **Q:** How to name the config file? **A:** `codegen.yml` (default)
    8. **Q:** What script in package.json should run the codegen? **A:** `generate`
6. `yarn install` to install the plugins
7. create `src/example.graphql` with the following content:
```
query blockByHeight($coin: String!, $height: Int!) {
  coin(name: $coin) {
    blockByHeight(height: $height) {
      block {
        height
        hash
        txCount
      }
    }
  }
}
```
8. `yarn generate` to generate TypeScript code from the graphql schema and queries
9. edit `src/index.tsx` to make ApolloClient available in react components:
```typescript
import React from 'react';
import ReactDOM from 'react-dom';
import './index.css';
import App from './App';
import reportWebVitals from './reportWebVitals';
import { ApolloClient, InMemoryCache, ApolloProvider } from '@apollo/client';

const client = new ApolloClient({
  uri: 'https://blockchaingraphql.com',
  cache: new InMemoryCache()
});

ReactDOM.render(
  <React.StrictMode>
    <ApolloProvider client={client}>
        <App />
    </ApolloProvider>
  </React.StrictMode>,
  document.getElementById('root')
);

// If you want to start measuring performance in your app, pass a function
// to log results (for example: reportWebVitals(console.log))
// or send to an analytics endpoint. Learn more: https://bit.ly/CRA-vitals
reportWebVitals();
```
10. edit `src/App.tsx` to query and render information about a block
```typescript
import './App.css';
import { useBlockByHeightQuery } from './generated/graphql';

function App() {
  const { data, error, loading } = useBlockByHeightQuery(
    {
      variables: {coin: "bitcoin", height: 100000}
    }
  );
  if (loading) return <p>Loading ...</p>;
  return <div>
    <p>height: {data?.coin?.blockByHeight?.block.height}</p>
    <p>hash: {data?.coin?.blockByHeight?.block.hash}</p>
    <p>transaction count: {data?.coin?.blockByHeight?.block.txCount}</p>
  </div>;
}

export default App;
```
11. `yarn start` to run the app. It should open a browser displaying:
```
height: 100000

hash: 000000000003ba27aa200b1cecaad478d2b00432346c3f1f3986da1afd33e506

transaction count: 4
```
