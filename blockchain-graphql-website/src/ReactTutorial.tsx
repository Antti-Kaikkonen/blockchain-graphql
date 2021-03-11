import { Container } from '@material-ui/core';
import {
    Link
} from "react-router-dom";
function ReactTutorial() {
    return <Container maxWidth="lg">
        <Link to="/tutorials/react-yarn">React tutorial using Yarn package manager</Link>
        <br />
        <Link to="/tutorials/react-npm">React tutorial using npm package manager</Link>
    </Container>
}

export default ReactTutorial;