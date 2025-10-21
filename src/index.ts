import express from 'express';
import http from 'http';
import path from 'path';
import { WebSocketServer } from 'ws';
// tslint:disable-next-line:no-var-requires
const { SpeechClient } = require('@google-cloud/speech').v2;

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

const speechClient = new SpeechClient({
    keyFilename: path.join(__dirname, '../gcp-credentials.json')
});

app.use(express.static(path.join(__dirname, '../public')));

wss.on('connection', (ws) => {
    console.log('Client connected');

    const recognizer = 'projects/signvrse/locations/global/recognizers/_';

    const streamingConfig = {
        config: {
            autoDecodingConfig: {},
            model: 'long',
            languageCodes: ['en-US'],
            features: {
                enableAutomaticPunctuation: true,
            },
        },
        streamingFeatures: {
            interimResults: true,
        },
    };

    const request = {
        recognizer,
        streamingConfig,
    };

    const stream = speechClient.streamingRecognize(request)
        .on('error', (error: Error) => {
            console.error('Speech API error:', error);
            ws.close();
        })
        .on('data', (data: any) => {
            if (data.results && data.results.length > 0) {
                const transcript = data.results[0].alternatives[0].transcript;
                const isFinal = data.results[0].isFinal;
                ws.send(JSON.stringify({ transcript, isFinal }));
            }
        });

    ws.on('message', (message) => {
        stream.write({ audio: message });
    });

    ws.on('close', () => {
        console.log('Client disconnected');
        stream.end();
    });
});

const PORT = process.env.PORT || 3000;

server.listen(PORT, () => {
    console.log(`Server is listening on port ${PORT}`);
});
