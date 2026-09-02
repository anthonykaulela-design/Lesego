/**
 * ============================================================================
 * LESEGO MARKETS - cTrader Open API Production Engine
 * Target Platform: Render Cloud / Node.js >= 18
 * Protocol Standard: Official Binary Protobuf (@spotware/open-api-sdk)
 * ============================================================================
 */

const express = require('express');
const cors = require('cors');
const axios = require('axios');
const http = require('http');
const WebSocket = require('ws');
const { CTraderConnection, ProtoOAPayloadType } = require('@spotware/open-api-sdk');
require('dotenv').config();

// ============================================================================
// SECTION 1: GLOBAL PROCESS GUARDS & UNCAUGHT ERROR SHIELDS
// ============================================================================

process.on('unhandledRejection', (reason, promise) => {
    console.error('[Crash Guard] Intercepted Unhandled Promise Rejection:', reason?.message || reason);
});

process.on('uncaughtException', (err) => {
    console.error('[Crash Guard] Intercepted Fatal Uncaught Exception:', err.message);
});

// ============================================================================
// SECTION 2: SYSTEM CONFIGURATION & ENVIRONMENT VARIABLES
// ============================================================================

const PORT = process.env.PORT || 10000;
const CTRADER_CLIENT_ID = process.env.CTRADER_CLIENT_ID || "38384_wh6ecCD5h0tHjsNXc57f7a0f2aZeKUubeFlpkKDMpQqHn58H0m";
const CTRADER_CLIENT_SECRET = process.env.CTRADER_CLIENT_SECRET || "jkcXDForVeNulNasjMa1vnQKtZwbrOLjgH4GDLL3dkVWZVC0V4";
const CTRADER_REDIRECT_URI = process.env.CTRADER_REDIRECT_URI || "https://lesego.onrender.com";

// Port 5035 for direct TCP/TLS stream sockets with CTraderConnection
const CTRADER_SERVERS = {
    DEMO: { host: 'demo.ctraderapi.com', port: 5035 },
    LIVE: { host: 'live.ctraderapi.com', port: 5035 }
};

// ============================================================================
// SECTION 3: EXPRESS & HTTP SERVER INITIALIZATION
// ============================================================================

const app = express();
const server = http.createServer(app);

app.use(cors({
    origin: '*',
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization', 'X-Requested-With']
}));

app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));

// ============================================================================
// SECTION 4: FRONTEND WEBSOCKET BROADCAST SERVER
// ============================================================================

const wssFrontend = new WebSocket.Server({ noServer: true });

server.on('upgrade', (request, socket, head) => {
    wssFrontend.handleUpgrade(request, socket, head, (ws) => {
        wssFrontend.emit('connection', ws, request);
    });
});

function broadcastToClients(data) {
    const payload = JSON.stringify(data);
    wssFrontend.clients.forEach((client) => {
        if (client.readyState === WebSocket.OPEN) {
            client.send(payload);
        }
    });
}

wssFrontend.on('connection', (ws) => {
    console.log('[Frontend WS] Web Application Client Connected.');
    ws.send(JSON.stringify({ type: 'SYSTEM_STATUS', status: 'ONLINE', message: 'Connected to Lesego Engine Bridge' }));

    ws.on('message', (message) => {
        try {
            const parsed = JSON.parse(message);
            console.log('[Frontend WS] Incoming Client Event:', parsed.type);
        } catch (e) {
            // Ignore malformed frames
        }
    });

    ws.on('error', (err) => {
        console.error('[Frontend WS] Client Connection Error:', err.message);
    });
});

// ============================================================================
// SECTION 5: PROTOBUF CTRADER API GATEWAY ENGINE
// ============================================================================

class ResilientCTraderGateway {
    constructor(environment = 'DEMO') {
        this.environment = environment.toUpperCase();
        this.serverConfig = CTRADER_SERVERS[this.environment];
        this.connection = null;
        this.isAuthorized = false;
        this.isConnecting = false;
        this.pingTimer = null;
        this.pendingRequests = new Map();
        this.msgCounter = 1;
    }

    connect() {
        if (this.isConnecting) return;
        this.isConnecting = true;

        console.log(`[cTrader Gateway] Initializing ${this.environment} TCP Socket to ${this.serverConfig.host}:${this.serverConfig.port}...`);

        try {
            this.connection = new CTraderConnection({
                host: this.serverConfig.host,
                port: this.serverConfig.port
            });

            this.connection.on('open', () => {
                console.log(`[cTrader Gateway] ${this.environment} Binary Socket Established Successfully.`);
                this.isConnecting = false;
                this.startHeartbeatPing();
                this.authorizeAppCredentials();
            });

            this.connection.on('data', (data) => {
                this.handleIncomingMessage(data);
            });

            this.connection.on('error', (err) => {
                console.error(`[cTrader Gateway] ${this.environment} Socket Error:`, err.message);
                this.isConnecting = false;
            });

            this.connection.on('close', () => {
                console.warn(`[cTrader Gateway] Connection Terminated on ${this.environment}. Resetting and Retrying in 5s...`);
                this.cleanup();
                setTimeout(() => this.connect(), 5000);
            });

            this.connection.connect();
        } catch (err) {
            console.error(`[cTrader Gateway] Initialization Exception on ${this.environment}:`, err.message);
            this.cleanup();
            setTimeout(() => this.connect(), 5000);
        }
    }

    cleanup() {
        this.isAuthorized = false;
        this.isConnecting = false;
        this.stopHeartbeatPing();

        // Cancel all pending promise timeouts immediately on disconnect
        for (const [clientMsgId, reqObj] of this.pendingRequests.entries()) {
            clearTimeout(reqObj.timer);
            reqObj.reject(new Error(`cTrader connection lost before receiving response for ID: ${clientMsgId}`));
        }
        this.pendingRequests.clear();
    }

    startHeartbeatPing() {
        this.stopHeartbeatPing();
        // Send PROTO_OA_VERSION_REQ every 15s to keep TLS stream active
        this.pingTimer = setInterval(() => {
            if (this.connection) {
                this.sendCommand(ProtoOAPayloadType.PROTO_OA_VERSION_REQ, {}).catch(() => {});
            }
        }, 15000);
    }

    stopHeartbeatPing() {
        if (this.pingTimer) {
            clearInterval(this.pingTimer);
            this.pingTimer = null;
        }
    }

    async authorizeAppCredentials() {
        console.log(`[cTrader Gateway] Transmitting Application Authorization for ${this.environment}...`);
        try {
            const response = await this.sendCommand(ProtoOAPayloadType.PROTO_OA_APPLICATION_AUTH_REQ, {
                clientId: CTRADER_CLIENT_ID,
                clientSecret: CTRADER_CLIENT_SECRET
            });
            this.isAuthorized = true;
            console.log(`[cTrader Gateway] Application Authorized Successfully on ${this.environment}!`);
            return response;
        } catch (err) {
            console.error(`[cTrader Gateway] App Auth Failed on ${this.environment}:`, err.message);
            this.isAuthorized = false;
        }
    }

    sendCommand(payloadType, payload = {}) {
        return new Promise((resolve, reject) => {
            if (!this.connection) {
                return reject(new Error(`${this.environment} Gateway socket unavailable`));
            }

            const clientMsgId = `req_${Date.now()}_${this.msgCounter++}`;

            const timer = setTimeout(() => {
                if (this.pendingRequests.has(clientMsgId)) {
                    this.pendingRequests.delete(clientMsgId);
                    reject(new Error(`Request timeout for PayloadType: ${payloadType}`));
                }
            }, 10000);

            this.pendingRequests.set(clientMsgId, { resolve, reject, timer });

            this.connection.sendCommand(payloadType, payload, clientMsgId)
                .catch((err) => {
                    clearTimeout(timer);
                    this.pendingRequests.delete(clientMsgId);
                    reject(err);
                });
        });
    }

    handleIncomingMessage(data) {
        try {
            if (data.clientMsgId && this.pendingRequests.has(data.clientMsgId)) {
                const { resolve, timer } = this.pendingRequests.get(data.clientMsgId);
                clearTimeout(timer);
                this.pendingRequests.delete(data.clientMsgId);
                resolve(data);
                return;
            }

            if (
                data.payloadType === ProtoOAPayloadType.PROTO_OA_SPOT_EVENT ||
                data.payloadType === ProtoOAPayloadType.PROTO_OA_EXECUTION_EVENT ||
                data.payloadType === ProtoOAPayloadType.PROTO_OA_ACCOUNT_DISCONNECT_EVENT ||
                data.payloadType === ProtoOAPayloadType.PROTO_OA_MARGIN_CHANGED_EVENT
            ) {
                broadcastToClients({
                    environment: this.environment,
                    payloadType: data.payloadType,
                    data: data
                });
            }
        } catch (err) {
            console.error('[cTrader Gateway] Message Dispatch Error:', err.message);
        }
    }
}

// Global Gateway Instances
const gateways = {
    DEMO: new ResilientCTraderGateway('DEMO'),
    LIVE: new ResilientCTraderGateway('LIVE')
};

gateways.DEMO.connect();
gateways.LIVE.connect();

// ============================================================================
// SECTION 6: EXPRESS REST API CONTROLLERS
// ============================================================================

app.get('/health', (req, res) => {
    res.status(200).json({
        status: 'HEALTHY',
        engine: 'Lesego Markets Gateway',
        demoGateway: gateways.DEMO.isAuthorized ? 'AUTHORIZED' : 'CONNECTING',
        liveGateway: gateways.LIVE.isAuthorized ? 'AUTHORIZED' : 'CONNECTING',
        timestamp: new Date().toISOString()
    });
});

app.get('/', (req, res) => {
    res.status(200).send('Lesego Markets cTrader Open API High-Performance Engine Active.');
});

app.get('/api/auth/login-url', (req, res) => {
    const authUrl = `https://openapi.ctrader.com/apps/auth?client_id=${CTRADER_CLIENT_ID}&redirect_uri=${encodeURIComponent(CTRADER_REDIRECT_URI)}&scope=trading`;
    res.json({ success: true, authUrl });
});

app.post('/api/auth/token', async (req, res) => {
    const { code } = req.body;
    if (!code) return res.status(400).json({ success: false, message: 'OAuth Code is required' });

    try {
        const response = await axios.get('https://openapi.ctrader.com/apps/token', {
            params: {
                grant_type: 'authorization_code',
                code: code,
                client_id: CTRADER_CLIENT_ID,
                client_secret: CTRADER_CLIENT_SECRET,
                redirect_uri: CTRADER_REDIRECT_URI
            }
        });
        res.json({ success: true, auth: response.data });
    } catch (error) {
        res.status(500).json({ success: false, error: error.response?.data || error.message });
    }
});

app.post('/api/accounts/authenticate', async (req, res) => {
    const { accessToken, cTraderAccountId, environment = 'DEMO' } = req.body;
    if (!accessToken || !cTraderAccountId) {
        return res.status(400).json({ success: false, message: 'AccessToken and cTraderAccountId are required' });
    }

    const gw = gateways[environment.toUpperCase()];
    try {
        const result = await gw.sendCommand(
            ProtoOAPayloadType.PROTO_OA_ACCOUNT_AUTH_REQ,
            {
                accessToken: accessToken,
                ctTraderAccountId: parseInt(cTraderAccountId)
            }
        );
        res.json({ success: true, details: result });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

app.get('/api/trade/positions', async (req, res) => {
    const { cTraderAccountId, environment = 'DEMO' } = req.query;
    if (!cTraderAccountId) return res.status(400).json({ success: false, message: 'cTraderAccountId is required' });

    const gw = gateways[environment.toUpperCase()];
    try {
        const result = await gw.sendCommand(ProtoOAPayloadType.PROTO_OA_RECONCILE_REQ, {
            ctTraderAccountId: parseInt(cTraderAccountId)
        });
        res.json({ success: true, positions: result.position || [], orders: result.order || [] });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

// ============================================================================
// SECTION 7: SERVER STARTUP
// ============================================================================

server.listen(PORT, '0.0.0.0', () => {
    console.log(`===========================================================`);
    console.log(` Lesego Markets Engine Listening on: http://0.0.0.0:${PORT} `);
    console.log(`===========================================================`);
});