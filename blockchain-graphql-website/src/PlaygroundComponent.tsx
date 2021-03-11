import { store, Playground } from "graphql-playground-react";
import { Provider } from "react-redux";

function PlaygroundComponent() {

    //const state = store.getState();


    // get the current settings
    //const settings = getSettings(state);
    //console.log("SETTINGS = ",settings);
    // override defaults
    //settings["editor.theme"] = 'light';

    // dispatch settings string
    //store.dispatch(setSettingsString(JSON.stringify(settings, null, 2)));
    return <div id='playground-parent'>
        <Provider store={store}>
            <Playground endpoint="https://blockchaingraphql.com" />
        </Provider>
    </div>;
}

export default PlaygroundComponent;