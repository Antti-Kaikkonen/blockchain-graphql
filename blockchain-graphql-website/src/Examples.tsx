import { Playground, rootReducer } from "graphql-playground-react";
import rootSaga from "graphql-playground-react/lib/state/rootSaga";
import { Tab } from "graphql-playground-react/lib/state/sessions/reducers";
import { Provider } from "react-redux";
import { createStore, compose, applyMiddleware } from 'redux';
import createSagaMiddleware from 'redux-saga'


const endpoint = "https://blockchaingraphql.com";
const available_coins_query: Tab = {
    endpoint: endpoint, name: "Available Coins", query:
        "{\n" +
        "  coins {\n" +
        "    name\n" +
        "  }\n" +
        "}\n"
};

const search_txid_from_all_coins_query: Tab = {
    endpoint: endpoint, name: "Search txid", query:
        "{\n" +
        "  coins {\n" +
        "    name\n" +
        "    transaction(\n" +
        "      txid: \"81335877f5ba07432f642206de7bb30367e3da8fa48dc91139795ecb6571e39e\"\n" +
        "    ) {\n" +
        "      height\n" +
        "      size\n" +
        "      fee\n" +
        "    }\n" +
        "  }\n" +
        "}\n"
};

const query_with_variables: Tab = {
    endpoint: endpoint, name: "Query variables", variables: "{\n\t\"coin\": \"bitcoin\",\n\t\"height\": 100000\n}",
    query:
        "#Click 'QUERY VARIABLES' in the bottom left corner to edit $coin and $height variables\n" +
        "query blockByHeight($coin: String!, $height: Int!) {\n" +
        "  coin(name: $coin) {\n" +
        "    blockByHeight(height: $height) {\n" +
        "      block {\n" +
        "        height\n" +
        "        hash\n" +
        "        txCount\n" +
        "      }\n" +
        "    }\n" +
        "  }\n" +
        "}\n"
};

const daily_richlist_top_gainers_and_top_losers_query: Tab = {
    endpoint: endpoint, name: "Richlist, Gainers and Losers", variables: "{\n\t\"coin\": \"bitcoin\",\n\t\"date\": \"2020-01-01\"\n}",
    query:
        "#Click 'QUERY VARIABLES' in the bottom left corner to edit $coin and $date variables\n" +
        "query($coin: String!, $date: String!) {\n" +
        "  coin(name: $coin) {\n" +
        "    date(date: $date) {\n" +
        "      richList(limit: 3) {\n" +
        "        items {\n" +
        "          balance\n" +
        "          address {\n" +
        "            address\n" +
        "          }\n" +
        "        }\n" +
        "      }\n" +
        "      topGainers(limit: 3) {\n" +
        "        items {\n" +
        "          balanceChange\n" +
        "          address {\n" +
        "            address\n" +
        "          }\n" +
        "        }\n" +
        "      },\n" +
        "      topLosers(limit: 3) {\n" +
        "        items {\n" +
        "          balanceChange\n" +
        "          address {\n" +
        "            address\n" +
        "          }\n" +
        "        }\n" +
        "      }\n" +
        "    }\n" +
        "  }\n" +
        "}\n"
};

const address_transactions: Tab = {
    endpoint: endpoint, name: "Address transactions", query:
        "{\n" +
        "  coin(name: \"bitcoin\") {\n" +
        "    name\n" +
        "    address(address: \"1GNHpGFNgZ163D8wqZos1QRfS3V2xyommN\") {\n" +
        "      confirmedTransactions(limit: 100) {\n" +
        "        items {\n" +
        "          height\n" +
        "          txN\n" +
        "          balanceChange\n" +
        "          balanceAfterBlock\n" +
        "        }\n" +
        "        hasMore\n" +
        "      }\n" +
        "    }\n" +
        "  }\n" +
        "}\n"
};

const address_cluster_transactions: Tab = {
    endpoint: endpoint, name: "Cluster transactions", query:
        "{\n" +
        "  coin(name: \"bitcoin\") {\n" +
        "    name\n" +
        "    address(address: \"1GNHpGFNgZ163D8wqZos1QRfS3V2xyommN\") {\n" +
        "      guestimatedWallet {\n" +
        "        clusterId\n" +
        "        transactions(limit: 100) {\n" +
        "          items {\n" +
        "            height\n" +
        "            txN\n" +
        "            balanceChange\n" +
        "          }\n" +
        "          hasMore\n" +
        "        }\n" +
        "      }\n" +
        "    }\n" +
        "  }\n" +
        "}\n"
};


const pagination: Tab = {
    endpoint: endpoint, name: "Pagination", query:
        "{\n" +
        "  coin(name: \"bitcoin\") {\n" +
        "    date(date: \"2017-01-01\") {\n" +
        "      topGainers(\n" +
        "        limit: 5\n" +
        "        cursor: {\n" +
        "          balanceChange: 100\n" +
        "          address: \"18KJEvGvtxSvoiGSNPLDB5hpYaQWJhpvt8\"\n" +
        "        }\n" +
        "      ) {\n" +
        "        items {\n" +
        "          balanceChange\n" +
        "          address {\n" +
        "            address\n" +
        "          }\n" +
        "        }\n" +
        "        hasMore\n" +
        "      }\n" +
        "    }\n" +
        "  }\n" +
        "}\n"
};

const composeEnhancers = (window as any).__REDUX_DEVTOOLS_EXTENSION_COMPOSE__ || compose
const sagaMiddleware = createSagaMiddleware()
const functions = [applyMiddleware(sagaMiddleware)]

function reducerWrapper(state: any, action: any) {
    return rootReducer(state, action);
}

const tmpStore = createStore(
    reducerWrapper, undefined, composeEnhancers.apply(null, functions)
);
sagaMiddleware.run(rootSaga);

const tabs: Tab[] = [available_coins_query, search_txid_from_all_coins_query, query_with_variables, daily_richlist_top_gainers_and_top_losers_query, address_transactions, address_cluster_transactions, pagination];

function Examples() {

    return <div id='playground-parent'>
        <Provider store={tmpStore}>
            <Playground tabs={tabs} />
        </Provider>
    </div>;
}

export default Examples;