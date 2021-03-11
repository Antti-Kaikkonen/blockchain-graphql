import React, { Component } from 'react';
import ReactMarkdown from 'react-markdown';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import { dark } from 'react-syntax-highlighter/dist/esm/styles/prism'
import { Container } from '@material-ui/core';

class ReactTutorialYarn extends Component<{}, { terms: string }> {
    constructor(props: { terms: string }) {
        super(props);
        this.state = { terms: "" };
    }


    componentWillMount() {
        fetch('/tutorials/React-Yarn.md').then((response) => response.text()).then((text) => {
            this.setState({ terms: text })
        })
    }

    render() {

        const renderers = {
            code: ({ language, value }: { language: string, value: string }) => {

                return <SyntaxHighlighter style={dark} language={language} children={value} />
            }
        }

        return (
            <Container maxWidth="lg">
                <ReactMarkdown source={this.state.terms} renderers={renderers} />
            </Container>
        )
    }
}

export default ReactTutorialYarn;