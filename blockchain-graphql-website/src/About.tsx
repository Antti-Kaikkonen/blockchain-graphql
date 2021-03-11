import { Container } from '@material-ui/core';
import high_level_flow from './high-level-flow.svg';
function About() {
    return <Container maxWidth="lg">
        <p>This site provides a GraphQL API for blockchain data of several crypto-currencies.</p>
        <p>
            <b>Important:</b> It is not recommended to rely on this service for anything critical.
      The API is still under devepment and backwards incompatible changes are possible.
      Furthermore I'm hosting this service from my home server and occasional interruptions to the service are to be expected.
      If you need stability and high availabilty you can check the github link and run the API on your own servers.
    </p>
        <h3>Features</h3>
        <ul>
            <li>Designed for high performance. Queries don't involve filtering and they are powered by a database that can be scaled out to meet the required throughput and latency requirements</li>
            <li>Query multiple blockchains from a single API. Currently supported: Bitcoin, Bitcoin Cash, Dash and Litecoin</li>
            <li>Blocks, Transactions, Inputs, Outputs</li>
            <li>Mempool data</li>
            <ul>
                <li>Transaction count</li>
                <li>Total in fees</li>
                <li>Transactions</li>
            </ul>
            <li>Adressress</li>
            <ul>
                <li>Confirmed transactions</li>
                <li>Unconfirmed transactions</li>
                <li>Daily balance OHLC data (Open, High, Low, Close)</li>
                <li>Guestimated wallet (a.k.a address clustering)</li>
                <ul>
                    <li>Addresses</li>
                    <li>Confirmed transactions with a few blocks delay</li>
                    <li>Daily balance changes</li>
                </ul>
            </ul>
            <li>Date</li>
            <ul>
                <li>Address rich list</li>
                <li>Top address gainers</li>
                <li>Top address losers</li>
                <li>Top cluster gainers</li>
                <li>Top cluster losers</li>
            </ul>
            <li>Cluster rich list</li>
        </ul>

        <h3>How does it works?</h3>
        <img src={high_level_flow} alt="High level data flow" style={{ 'maxHeight': '100%', 'maxWidth': '100%' }} />
        <p>
            For bitcoin the state stored in flink is 300GB+ and the data stored in Scylla takes ~1.5TB.
    </p>
    TODO: Create a tutorial on how to run the Flink program and the GraphQL API
  </Container>;
}

export default About;