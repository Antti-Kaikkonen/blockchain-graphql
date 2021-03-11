import React from 'react';
import clsx from 'clsx';
import { makeStyles, useTheme } from '@material-ui/core/styles';
import Drawer from '@material-ui/core/Drawer';
import CssBaseline from '@material-ui/core/CssBaseline';
import AppBar from '@material-ui/core/AppBar';
import Toolbar from '@material-ui/core/Toolbar';
import List from '@material-ui/core/List';
import Typography from '@material-ui/core/Typography';
import Divider from '@material-ui/core/Divider';
import IconButton from '@material-ui/core/IconButton';
import MenuIcon from '@material-ui/icons/Menu';
import ChevronLeftIcon from '@material-ui/icons/ChevronLeft';
import ChevronRightIcon from '@material-ui/icons/ChevronRight';
import ListItem from '@material-ui/core/ListItem';
import ListItemIcon from '@material-ui/core/ListItemIcon';
import ListItemText from '@material-ui/core/ListItemText';
import EmailIcon from '@material-ui/icons/Email';
import InfoIcon from '@material-ui/icons/Info';
import HomeIcon from '@material-ui/icons/Home';
import ListIcon from '@material-ui/icons/List';
import PlaygroundComponent from './PlaygroundComponent';
import About from './About';
import SvgIcon from '@material-ui/core/SvgIcon';

import {
    BrowserRouter as Router,
    Switch,
    Route,
    Link
} from "react-router-dom";
import Examples from './Examples';
import AngularTutorialYarn from './AngularTutorialYarn';
import ReactTutorialYarn from './ReactTutorialYarn';
import ReactTutorial from './ReactTutorial';
import ReactTutorialNpm from './ReactTutorialNpm';
import AngularTutorial from './AngularTutorial';
import AngularTutorialNpm from './AngularTutorialNpm';

const drawerWidth = 240;

const useStyles = makeStyles((theme) => ({
    root: {
        display: 'flex',
        height: '100%'
    },
    appBar: {
        transition: theme.transitions.create(['margin', 'width'], {
            easing: theme.transitions.easing.sharp,
            duration: theme.transitions.duration.leavingScreen,
        }),
        flexGrow: 0
    },
    appBarShift: {
        width: `calc(100% - ${drawerWidth}px)`,
        marginLeft: drawerWidth,
        transition: theme.transitions.create(['margin', 'width'], {
            easing: theme.transitions.easing.easeOut,
            duration: theme.transitions.duration.enteringScreen,
        }),
    },
    menuButton: {
        marginRight: theme.spacing(2),
    },
    beta: {
        marginLeft: theme.spacing(1),
        flexGrow: 1
    },
    title: {
        flexGrow: 0
    },
    hide: {
        display: 'none',
    },
    drawer: {
        width: drawerWidth,
        flexShrink: 0,
    },
    drawerPaper: {
        width: drawerWidth,
    },
    drawerHeader: {
        display: 'flex',
        alignItems: 'center',
        padding: theme.spacing(0, 1),
        // necessary for content to be below app bar
        ...theme.mixins.toolbar,
        justifyContent: 'flex-end',
    },
    content: {
        flexGrow: 1,
        padding: theme.spacing(0),
        transition: theme.transitions.create('margin', {
            easing: theme.transitions.easing.sharp,
            duration: theme.transitions.duration.leavingScreen,
        }),
        marginLeft: -drawerWidth,
        display: 'flex',
        flexDirection: 'column'
    },
    contentShift: {
        transition: theme.transitions.create('margin', {
            easing: theme.transitions.easing.easeOut,
            duration: theme.transitions.duration.enteringScreen,
        }),
        marginLeft: 0,
    },
    innerContent: {
        flexGrow: 1,
    }
}));

function GithubIcon(props: any) {
    return (
        <SvgIcon {...props}>
            <path d="M12 .3a12 12 0 0 0-3.8 23.4c.6.1.8-.3.8-.6v-2c-3.3.7-4-1.6-4-1.6-.6-1.4-1.4-1.8-1.4-1.8-1-.7.1-.7.1-.7 1.2 0 1.9 1.2 1.9 1.2 1 1.8 2.8 1.3 3.5 1 0-.8.4-1.3.7-1.6-2.7-.3-5.5-1.3-5.5-6 0-1.2.5-2.3 1.3-3.1-.2-.4-.6-1.6 0-3.2 0 0 1-.3 3.4 1.2a11.5 11.5 0 0 1 6 0c2.3-1.5 3.3-1.2 3.3-1.2.6 1.6.2 2.8 0 3.2.9.8 1.3 1.9 1.3 3.2 0 4.6-2.8 5.6-5.5 5.9.5.4.9 1 .9 2.2v3.3c0 .3.1.7.8.6A12 12 0 0 0 12 .3"></path>
        </SvgIcon>
    );
}


function AngularIcon() {
    return (
        <SvgIcon viewBox="0 0 250 250">
            <g>
                <polygon fill="#DD0031" points="125,30 125,30 125,30 31.9,63.2 46.1,186.3 125,230 125,230 125,230 203.9,186.3 218.1,63.2  " />
                <polygon fill="#C3002F" points="125,30 125,52.2 125,52.1 125,153.4 125,153.4 125,230 125,230 203.9,186.3 218.1,63.2 125,30  " />
                <path fill="#FFFFFF" d="M125,52.1L66.8,182.6h0h21.7h0l11.7-29.2h49.4l11.7,29.2h0h21.7h0L125,52.1L125,52.1L125,52.1L125,52.1   L125,52.1z M142,135.4H108l17-40.9L142,135.4z" />
            </g>
        </SvgIcon>
    )
}


function ReactIcon() {
    return (
        <SvgIcon viewBox="0 0 490.59999 436.9">
            <g
                id="g10"
                transform="translate(-175.7,-78)">
                <path fill="#61DAFB"
                    d="m 666.3,296.5 c 0,-32.5 -40.7,-63.3 -103.1,-82.4 14.4,-63.6 8,-114.2 -20.2,-130.4 -6.5,-3.8 -14.1,-5.6 -22.4,-5.6 v 22.3 c 4.6,0 8.3,0.9 11.4,2.6 13.6,7.8 19.5,37.5 14.9,75.7 -1.1,9.4 -2.9,19.3 -5.1,29.4 -19.6,-4.8 -41,-8.5 -63.5,-10.9 -13.5,-18.5 -27.5,-35.3 -41.6,-50 32.6,-30.3 63.2,-46.9 84,-46.9 V 78 c 0,0 0,0 0,0 -27.5,0 -63.5,19.6 -99.9,53.6 C 384.4,97.8 348.4,78.4 320.9,78.4 v 22.3 c 20.7,0 51.4,16.5 84,46.6 -14,14.7 -28,31.4 -41.3,49.9 -22.6,2.4 -44,6.1 -63.6,11 -2.3,-10 -4,-19.7 -5.2,-29 -4.7,-38.2 1.1,-67.9 14.6,-75.8 3,-1.8 6.9,-2.6 11.5,-2.6 V 78.5 c 0,0 0,0 0,0 -8.4,0 -16,1.8 -22.6,5.6 -28.1,16.2 -34.4,66.7 -19.9,130.1 -62.2,19.2 -102.7,49.9 -102.7,82.3 0,32.5 40.7,63.3 103.1,82.4 -14.4,63.6 -8,114.2 20.2,130.4 6.5,3.8 14.1,5.6 22.5,5.6 27.5,0 63.5,-19.6 99.9,-53.6 36.4,33.8 72.4,53.2 99.9,53.2 8.4,0 16,-1.8 22.6,-5.6 28.1,-16.2 34.4,-66.7 19.9,-130.1 62,-19.1 102.5,-49.9 102.5,-82.3 z M 536.1,229.8 c -3.7,12.9 -8.3,26.2 -13.5,39.5 -4.1,-8 -8.4,-16 -13.1,-24 -4.6,-8 -9.5,-15.8 -14.4,-23.4 14.2,2.1 27.9,4.7 41,7.9 z m -45.8,106.5 c -7.8,13.5 -15.8,26.3 -24.1,38.2 -14.9,1.3 -30,2 -45.2,2 -15.1,0 -30.2,-0.7 -45,-1.9 -8.3,-11.9 -16.4,-24.6 -24.2,-38 -7.6,-13.1 -14.5,-26.4 -20.8,-39.8 6.2,-13.4 13.2,-26.8 20.7,-39.9 7.8,-13.5 15.8,-26.3 24.1,-38.2 14.9,-1.3 30,-2 45.2,-2 15.1,0 30.2,0.7 45,1.9 8.3,11.9 16.4,24.6 24.2,38 7.6,13.1 14.5,26.4 20.8,39.8 -6.3,13.4 -13.2,26.8 -20.7,39.9 z m 32.3,-13 c 5.4,13.4 10,26.8 13.8,39.8 -13.1,3.2 -26.9,5.9 -41.2,8 4.9,-7.7 9.8,-15.6 14.4,-23.7 4.6,-8 8.9,-16.1 13,-24.1 z M 421.2,430 c -9.3,-9.6 -18.6,-20.3 -27.8,-32 9,0.4 18.2,0.7 27.5,0.7 9.4,0 18.7,-0.2 27.8,-0.7 -9,11.7 -18.3,22.4 -27.5,32 z m -74.4,-58.9 c -14.2,-2.1 -27.9,-4.7 -41,-7.9 3.7,-12.9 8.3,-26.2 13.5,-39.5 4.1,8 8.4,16 13.1,24 4.7,8 9.5,15.8 14.4,23.4 z M 420.7,163 c 9.3,9.6 18.6,20.3 27.8,32 -9,-0.4 -18.2,-0.7 -27.5,-0.7 -9.4,0 -18.7,0.2 -27.8,0.7 9,-11.7 18.3,-22.4 27.5,-32 z m -74,58.9 c -4.9,7.7 -9.8,15.6 -14.4,23.7 -4.6,8 -8.9,16 -13,24 -5.4,-13.4 -10,-26.8 -13.8,-39.8 13.1,-3.1 26.9,-5.8 41.2,-7.9 z m -90.5,125.2 c -35.4,-15.1 -58.3,-34.9 -58.3,-50.6 0,-15.7 22.9,-35.6 58.3,-50.6 8.6,-3.7 18,-7 27.7,-10.1 5.7,19.6 13.2,40 22.5,60.9 -9.2,20.8 -16.6,41.1 -22.2,60.6 -9.9,-3.1 -19.3,-6.5 -28,-10.2 z M 310,490 c -13.6,-7.8 -19.5,-37.5 -14.9,-75.7 1.1,-9.4 2.9,-19.3 5.1,-29.4 19.6,4.8 41,8.5 63.5,10.9 13.5,18.5 27.5,35.3 41.6,50 -32.6,30.3 -63.2,46.9 -84,46.9 -4.5,-0.1 -8.3,-1 -11.3,-2.7 z m 237.2,-76.2 c 4.7,38.2 -1.1,67.9 -14.6,75.8 -3,1.8 -6.9,2.6 -11.5,2.6 -20.7,0 -51.4,-16.5 -84,-46.6 14,-14.7 28,-31.4 41.3,-49.9 22.6,-2.4 44,-6.1 63.6,-11 2.3,10.1 4.1,19.8 5.2,29.1 z m 38.5,-66.7 c -8.6,3.7 -18,7 -27.7,10.1 -5.7,-19.6 -13.2,-40 -22.5,-60.9 9.2,-20.8 16.6,-41.1 22.2,-60.6 9.9,3.1 19.3,6.5 28.1,10.2 35.4,15.1 58.3,34.9 58.3,50.6 -0.1,15.7 -23,35.6 -58.4,50.6 z"
                />
                <polygon fill="#61DAFB"
                    points=""
                />
                <circle
                    cx="420.89999"
                    cy="296.5"
                    r="45.700001"
                    fill="#61DAFB" />
                <polygon
                    points=""
                    fill="#61DAFB" />
            </g>
        </SvgIcon>
    )
}

function App() {

    const classes = useStyles();
    const theme = useTheme();
    const [open, setOpen] = React.useState(false);

    const handleDrawerOpen = () => {
        setOpen(true);
    };

    const handleDrawerClose = () => {
        setOpen(false);
    };

    return (
        <div className={classes.root}>

            <Router>

                <CssBaseline />
                <AppBar
                    position="fixed"
                    className={clsx(classes.appBar, {
                        [classes.appBarShift]: open,
                    })}
                >
                    <Toolbar>
                        <IconButton
                            color="inherit"
                            aria-label="open drawer"
                            onClick={handleDrawerOpen}
                            edge="start"
                            className={clsx(classes.menuButton, open && classes.hide)}
                        >
                            <MenuIcon />
                        </IconButton>
                        <Typography variant="h6" noWrap className={classes.title}>
                            BlockchainGraphQL
            </Typography>
                        <Typography variant="body1" color="secondary" noWrap className={classes.beta}>Beta</Typography>
                        <IconButton aria-label="GitHub repository" href="https://github.com/Antti-Kaikkonen/blockchain-graphql" target="_blank" color="inherit">
                            <GithubIcon />
                        </IconButton>
                    </Toolbar>
                </AppBar>
                <Drawer
                    className={classes.drawer}
                    variant="persistent"
                    anchor="left"
                    open={open}
                    classes={{
                        paper: classes.drawerPaper,
                    }}
                >
                    <div className={classes.drawerHeader}>
                        <IconButton onClick={handleDrawerClose}>
                            {theme.direction === 'ltr' ? <ChevronLeftIcon /> : <ChevronRightIcon />}
                        </IconButton>
                    </div>
                    <Divider />
                    <List>
                        <ListItem button key="Playground" component={Link} to="/">
                            <ListItemIcon>
                                <HomeIcon />
                            </ListItemIcon>
                            <ListItemText primary="Playground" />
                        </ListItem>
                        <ListItem button key="Example queries" component={Link} to="/examples">
                            <ListItemIcon>
                                <ListIcon />
                            </ListItemIcon>
                            <ListItemText primary="Example queries" />
                        </ListItem>
                        <ListItem button key="About" component={Link} to="/about">
                            <ListItemIcon>
                                <InfoIcon />
                            </ListItemIcon>
                            <ListItemText primary="About" />
                        </ListItem>
                        <ListItem button key="Angular tutorial" component={Link} to="/tutorials/angular">
                            <ListItemIcon>
                                <AngularIcon />
                            </ListItemIcon>
                            <ListItemText primary="Angular tutorial" />
                        </ListItem>
                        <ListItem button key="React tutorial" component={Link} to="/tutorials/react">
                            <ListItemIcon>
                                <ReactIcon />
                            </ListItemIcon>
                            <ListItemText primary="React tutorial" />
                        </ListItem>
                        <ListItem button key="Contact" component="a" href="mailto:contact@blockchaingraphql.com">
                            <ListItemIcon>
                                <EmailIcon />
                            </ListItemIcon>
                            <ListItemText primary="Contact" />
                        </ListItem>
                    </List>
                </Drawer>
                <main
                    className={clsx(classes.content, {
                        [classes.contentShift]: open,
                    })}
                >
                    <div className={classes.drawerHeader} />
                    <div className={classes.innerContent}>
                        <Switch>
                            <Route path="/examples">
                                <Examples />
                            </Route>
                            <Route path="/about">
                                <About />
                            </Route>
                            <Route path="/tutorials/angular">
                                <AngularTutorial />
                            </Route>
                            <Route path="/tutorials/angular-yarn">
                                <AngularTutorialYarn />
                            </Route>
                            <Route path="/tutorials/angular-npm">
                                <AngularTutorialNpm />
                            </Route>
                            <Route path="/tutorials/react">
                                <ReactTutorial />
                            </Route>
                            <Route path="/tutorials/react-yarn">
                                <ReactTutorialYarn />
                            </Route>
                            <Route path="/tutorials/react-npm">
                                <ReactTutorialNpm />
                            </Route>
                            <Route path="/">
                                <PlaygroundComponent />
                            </Route>
                        </Switch>
                    </div>
                </main>
            </Router>
        </div>
    );
}

export default App;
