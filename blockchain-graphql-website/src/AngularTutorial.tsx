import { Container } from '@material-ui/core';
import {
    Link
} from "react-router-dom";
function AngularTutorial() {
    return <Container maxWidth="lg">
        <Link to="/tutorials/angular-yarn">Angular tutorial using Yarn package manager</Link>
        <br />
        <Link to="/tutorials/angular-npm">Angular tutorial using npm package manager</Link>
    </Container>
}

export default AngularTutorial;