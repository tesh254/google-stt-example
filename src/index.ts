import express from "express";
import http from "http";
import path from "path";
import { WebSocketServer } from "ws";
// tslint:disable-next-line:no-var-requires
const { SpeechClient } = require("@google-cloud/speech").v2;
const ffmpeg = require("fluent-ffmpeg");

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

const speechClient = new SpeechClient({
  keyFilename: path.join(__dirname, "../gcp-credentials.json"),
});

app.use(express.static(path.join(__dirname, "../public")));

// Recognizer configuration
const projectId = "817689209292";
const location = "global";
const recognizerId = "terpstt";
const recognizerPath = `projects/${projectId}/locations/${location}/recognizers/${recognizerId}`;

async function ensureRecognizer() {
  try {
    // Check if recognizer exists
    const [recognizers] = await speechClient.listRecognizers({
      parent: `projects/${projectId}/locations/${location}`,
    });

    const recognizerExists = recognizers.some(
      (rec) => rec.name === recognizerPath
    );

    if (recognizerExists) {
      console.log(`Recognizer ${recognizerPath} already exists`);
      // Fetch recognizer details to verify configuration
      const [recognizer] = await speechClient.getRecognizer({
        name: recognizerPath,
      });
      console.log("Recognizer details:", JSON.stringify(recognizer, null, 2));

      // Check if recognizer configuration matches expected settings
      const expectedConfig = {
        languageCodes: ["en-US"],
        model: "long",
        features: { enableAutomaticPunctuation: true },
      };
      if (
        !recognizer.languageCodes.includes("en-US") ||
        recognizer.model !== "long" ||
        !recognizer.features?.enableAutomaticPunctuation
      ) {
        console.warn(
          "Recognizer configuration mismatch. Deleting and recreating recognizer."
        );
        await speechClient.deleteRecognizer({ name: recognizerPath });
        return await createRecognizer();
      }
      return recognizerPath;
    }

    // Create recognizer if it doesn't exist
    return await createRecognizer();
  } catch (error) {
    console.error("Error ensuring recognizer:", error);
    throw error;
  }
}

async function createRecognizer() {
  try {
    console.log(`Creating recognizer ${recognizerPath}`);
    const [operation] = await speechClient.createRecognizer({
      parent: `projects/${projectId}/locations/${location}`,
      recognizerId,
      recognizer: {
        languageCodes: ["en-US"],
        model: "long",
        displayName: "TerpSTT Recognizer",
        features: {
          enableAutomaticPunctuation: true,
        },
      },
    });

    const [response] = await operation.promise();
    console.log(`Recognizer created: ${response.name}`);
    return response.name;
  } catch (error) {
    console.error("Error creating recognizer:", error);
    throw error;
  }
}

// Initialize recognizer before starting WebSocket connections
let recognizer;

(async () => {
  try {
    recognizer = await ensureRecognizer();
  } catch (error) {
    console.error("Failed to initialize recognizer, shutting down server");
    process.exit(1);
  }

  wss.on("connection", (ws) => {
    console.log("Client connected");

    let streamActive = true; // Track stream state

    const streamingConfig = {
      config: {
        autoDecodingConfig: {},
        model: "long",
        languageCodes: ["en-US"],
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

    console.log(
      "Starting streamingRecognize with config:",
      JSON.stringify(request, null, 2)
    );

    const stream = speechClient
      .streamingRecognize()
      .on("error", (error) => {
        console.error("Speech API error:", JSON.stringify(error, null, 2));
        ws.send(JSON.stringify({ error: error.message }));
        streamActive = false;
        ws.close();
      })
      .on("data", (data) => {
        if (data.results && data.results.length > 0) {
          const transcript = data.results[0].alternatives[0].transcript;
          const isFinal = data.results[0].isFinal;
          ws.send(JSON.stringify({ transcript, isFinal }));
        }
      })
      .on("end", () => {
        console.log("Speech API stream ended");
        streamActive = false;
      });

    // The first message must contain recognition config
    try {
      stream.write(request);
    } catch (error) {
      console.error("Error writing recognition config to stream:", error);
      ws.send(JSON.stringify({ error: "Failed to initialize stream" }));
      streamActive = false;
      ws.close();
    }

    ws.on("message", (message) => {
      console.log("Received audio message, length:", Buffer.isBuffer(message) ? message.length : (message as ArrayBuffer).byteLength);
      if (streamActive) {
        try {
          ffmpeg(message)
            .inputFormat("webm")
            .audioCodec("pcm_s16le")
            .audioFrequency(16000)
            .audioChannels(1)
            .format("wav")
            .on("error", (err) => {
              console.error("FFmpeg error:", err);
              ws.send(JSON.stringify({ error: "Audio processing error" }));
            })
            .pipe(stream, { end: false });
        } catch (error) {
          console.error("Error processing audio message:", error);
          ws.send(JSON.stringify({ error: "Audio processing error" }));
          streamActive = false;
          ws.close();
        }
      } else {
        console.log("Ignoring message: stream is not active");
      }
    });

    ws.on("close", () => {
      console.log("Client disconnected");
      if (streamActive) {
        stream.end();
        streamActive = false;
      }
    });
  });

  const PORT = process.env.PORT || 3000;

  server.listen(PORT, () => {
    console.log(`Server is listening on port ${PORT}`);
  });
})();
