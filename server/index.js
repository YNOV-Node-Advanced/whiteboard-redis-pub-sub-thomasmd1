const path = require("path");
const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const uuidv4 = require("uuid/v4");

const app = express();

const PUBLIC_FOLDER = path.join(__dirname, "../public");
const PORT = process.env.PORT || 5000;

const socketsPerChannels /* Map<string, Set<WebSocket>> */ = new Map();
const channelsPerSocket /* WeakMap<WebSocket, Set<string> */ = new WeakMap();

const redis = require("redis");
const redisPublisher = redis.createClient();
const redisSubscriber = redis.createClient();


// Initialize a simple http server
const server = http.createServer(app);

// Initialize the WebSocket server instance
const wss = new WebSocket.Server({ server });

/*
 * Subscribe a socket to a specific channel.
 */
function subscribe(socket, channel) {
  let socketSubscribed = socketsPerChannels.get(channel) || new Set();
  let channelSubscribed = channelsPerSocket.get(socket) || new Set();

  socketSubscribed = socketSubscribed.add(socket);
  channelSubscribed = channelSubscribed.add(channel);

  if (socketSubscribed.size === 1) {
    redisSubscriber.subscribe(channel);
  }

  socketsPerChannels.set(channel, socketSubscribed);
  channelsPerSocket.set(socket, channelSubscribed);

  client.set(channel);
}

/*
 * Unsubscribe a socket from a specific channel.
 */
function unsubscribe(socket, channel) {
  let socketSubscribed = socketsPerChannels.get(channel) || new Set();
  let channelSubscribed = channelsPerSocket.get(socket) || new Set();

  socketSubscribed.delete(socket);
  channelSubscribed.delete(channel);

  if (socketSubscribed.size === 0) {
    redisSubscriber.unsubscribe(channel);
  }

  socketsPerChannels.set(channel, socketSubscribed);
  channelsPerSocket.set(socket, channelSubscribed);
}

/*
 * Subscribe a socket from all channels.
 */
function unsubscribeAll(socket) {
  const channelSubscribed = channelsPerSocket.get(socket) || new Set();

  channelSubscribed.forEach(channel => {
    unsubscribe(socket, channel);
  });
}

/*
 * Broadcast a message to all sockets connected to this server.
 */
function broadcastToSockets(channel, data) {
  const socketSubscribed = socketsPerChannels.get(channel) || new Set();

  socketSubscribed.forEach(client => {
    client.send(data);
  });
}

// Broadcast message from client
wss.on("connection", ws => {
  ws.on("close", () => {
    unsubscribeAll(ws);
  });

  ws.on("message", data => {
    const message = JSON.parse(data.toString());

    switch (message.type) {
      case "subscribe":
        subscribe(ws, message.channel);
        break;
      default:
        broadcastToSockets(message.channel, data);
        break;
    }
  });
});

// Assign a random channel to people opening the application
app.get("/", (req, res) => {
  res.redirect(`/${uuidv4()}`);
});

app.get("/:channel", (req, res, next) => {
  res.sendFile(path.join(PUBLIC_FOLDER, "index.html"), {}, err => {
    if (err) {
      next(err);
    }
  });
});

app.use(express.static(PUBLIC_FOLDER));

server.listen(PORT, () => {
  console.log(`Server started on port ${server.address().port}`);
});
