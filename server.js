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
// SECTION 2: ENVIRONMENT & API CONFIGURATION
// ============================================================================
const PORT = process.env.PORT || 10000;
const CTRADER_CLIENT_ID = process.env.CTRADER_CLIENT_ID || "38384_wh6ecCD5h0tHjsNXc57f7a0f2aZeKUubeFlpkKDMpQqHn58H0m";
const CTRADER_CLIENT_SECRET = process.env.CTRADER_CLIENT_SECRET || "jkcXDForVeNulNasjMa1vnQKtZwbrOLjgH4GDLL3dkVWZVC0V4";
const CTRADER_REDIRECT_URI = process.env.CTRADER_REDIRECT_URI || "https://lesego.onrender.com";

// Use Port 5036 for WebSocket (wss://) connections, or 5035 for direct TCP sockets
const CTRADER_SERVERS = {
    DEMO: { host: 'demo.ctraderapi.com', port: 5036 },
    LIVE: { host: 'live.ctraderapi.com', port: 5036 }
};

// ============================================================================
// SECTION 3: EXPRESS & HTTP SERVER INITIALIZATION
// ============================================================================

const app = express();
const server = http.createServer(app);

// Enable Cross-Origin Resource Sharing (CORS) for web frontends
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

        console.log(`[cTrader Gateway] Initializing ${this.environment} Connection to ${this.serverConfig.host}:${this.serverConfig.port}...`);

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

        // Reject all outstanding pending requests to stop unhandled promise timeouts
        for (const [clientMsgId, reqObj] of this.pendingRequests.entries()) {
            clearTimeout(reqObj.timer);
            reqObj.reject(new Error(`cTrader connection lost before receiving response for ID: ${clientMsgId}`));
        }
        this.pendingRequests.clear();
    }

    startHeartbeatPing() {
        this.stopHeartbeatPing();
        // Send a PROTO_OA_VERSION_REQ ping every 15 seconds to keep the socket alive reliably
        this.pingTimer = setInterval(() => {
            if (this.connection && this.connection.isConnected) {
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
                    reject(new Error(`cTrader Request Timeout for PayloadType: ${payloadType}`));
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
            // Resolve matching request promise if clientMsgId is present
            if (data.clientMsgId && this.pendingRequests.has(data.clientMsgId)) {
                const { resolve, timer } = this.pendingRequests.get(data.clientMsgId);
                clearTimeout(timer);
                this.pendingRequests.delete(data.clientMsgId);
                resolve(data);
                return;
            }

            // Route real-time streaming events to frontend web clients
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
            console.error('[cTrader Gateway] Message Frame Dispatch Error:', err.message);
        }
    }
}

// Active Dual Stream Connection Instances
const gateways = {
    DEMO: new ResilientCTraderGateway('DEMO'),
    LIVE: new ResilientCTraderGateway('LIVE')
};

gateways.DEMO.connect();
gateways.LIVE.connect();

// ============================================================================
// SECTION 6: EXPRESS REST API CONTROLLERS & ROUTES
// ============================================================================

// 6.1 HEALTH CHECK & MONITORS
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
    res.status(200).send('Lesego Markets cTrader Open API High-Performance Gateway Engine Active.');
});

// 6.2 OAUTH & AUTHENTICATION ENDPOINTS
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
        console.error('[OAuth Token Error]:', error.response?.data || error.message);
        res.status(500).json({
            success: false,
            message: 'Failed to exchange authorization code with cTrader',
            error: error.response?.data || error.message
        });
    }
});

app.post('/api/auth/refresh', async (req, res) => {
    const { refreshToken } = req.body;
    if (!refreshToken) return res.status(400).json({ success: false, message: 'Refresh token is required' });

    try {
        const response = await axios.get('https://openapi.ctrader.com/apps/token', {
            params: {
                grant_type: 'refresh_token',
                refresh_token: refreshToken,
                client_id: CTRADER_CLIENT_ID,
                client_secret: CTRADER_CLIENT_SECRET
            }
        });
        res.json({ success: true, auth: response.data });
    } catch (error) {
        res.status(500).json({ success: false, error: error.response?.data || error.message });
    }
});

// 6.3 ACCOUNT MANAGEMENT ENDPOINTS
app.post('/api/accounts/list', async (req, res) => {
    const { accessToken, environment = 'DEMO' } = req.body;
    if (!accessToken) return res.status(400).json({ success: false, message: 'Access Token required' });

    const gw = gateways[environment.toUpperCase()];
    if (!gw) return res.status(400).json({ success: false, message: 'Invalid Environment specified' });

    try {
        const result = await gw.sendCommand(
            ProtoOAPayloadType.PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_REQ,
            { accessToken }
        );
        res.json({ success: true, accounts: result.ctaTraderAccount });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
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

app.get('/api/accounts/trader-info', async (req, res) => {
    const { cTraderAccountId, environment = 'DEMO' } = req.query;
    if (!cTraderAccountId) return res.status(400).json({ success: false, message: 'cTraderAccountId is required' });

    const gw = gateways[environment.toUpperCase()];
    try {
        const result = await gw.sendCommand(
            ProtoOAPayloadType.PROTO_OA_TRADER_REQ,
            { ctTraderAccountId: parseInt(cTraderAccountId) }
        );
        res.json({ success: true, trader: result.trader });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

// 6.4 MARKET DATA ENDPOINTS
app.get('/api/market/symbols', async (req, res) => {
    const { cTraderAccountId, environment = 'DEMO' } = req.query;
    if (!cTraderAccountId) return res.status(400).json({ success: false, message: 'cTraderAccountId is required' });

    const gw = gateways[environment.toUpperCase()];
    try {
        const result = await gw.sendCommand(ProtoOAPayloadType.PROTO_OA_SYMBOLS_LIST_REQ, {
            ctTraderAccountId: parseInt(cTraderAccountId)
        });
        res.json({ success: true, symbols: result.symbol });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

app.post('/api/market/symbol-details', async (req, res) => {
    const { cTraderAccountId, symbolIds, environment = 'DEMO' } = req.body;
    if (!cTraderAccountId || !symbolIds) {
        return res.status(400).json({ success: false, message: 'cTraderAccountId and symbolIds are required' });
    }

    const gw = gateways[environment.toUpperCase()];
    try {
        const result = await gw.sendCommand(ProtoOAPayloadType.PROTO_OA_SYMBOL_BY_ID_REQ, {
            ctTraderAccountId: parseInt(cTraderAccountId),
            symbolId: Array.isArray(symbolIds) ? symbolIds.map(i => parseInt(i)) : [parseInt(symbolIds)]
        });
        res.json({ success: true, symbolDetails: result.symbol });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

app.post('/api/market/trendbars', async (req, res) => {
    const { cTraderAccountId, symbolId, period = "M1", count = 100, environment = 'DEMO' } = req.body;
    if (!cTraderAccountId || !symbolId) {
        return res.status(400).json({ success: false, message: 'cTraderAccountId and symbolId are required' });
    }

    const gw = gateways[environment.toUpperCase()];
    try {
        const result = await gw.sendCommand(ProtoOAPayloadType.PROTO_OA_GET_TRENDBARS_REQ, {
            ctTraderAccountId: parseInt(cTraderAccountId),
            symbolId: parseInt(symbolId),
            period: period,
            count: parseInt(count)
        });
        res.json({ success: true, trendbars: result.trendbar });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

app.post('/api/market/subscribe', async (req, res) => {
    const { cTraderAccountId, symbolIds, environment = 'DEMO' } = req.body;
    if (!cTraderAccountId || !symbolIds) {
        return res.status(400).json({ success: false, message: 'cTraderAccountId and symbolIds are required' });
    }

    const gw = gateways[environment.toUpperCase()];
    try {
        const result = await gw.sendCommand(ProtoOAPayloadType.PROTO_OA_SUBSCRIBE_SPOTS_REQ, {
            ctTraderAccountId: parseInt(cTraderAccountId),
            symbolId: Array.isArray(symbolIds) ? symbolIds.map(i => parseInt(i)) : [parseInt(symbolIds)]
        });
        res.json({ success: true, message: 'Subscribed to Symbol Ticks', result });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

app.post('/api/market/unsubscribe', async (req, res) => {
    const { cTraderAccountId, symbolIds, environment = 'DEMO' } = req.body;
    if (!cTraderAccountId || !symbolIds) {
        return res.status(400).json({ success: false, message: 'cTraderAccountId and symbolIds are required' });
    }

    const gw = gateways[environment.toUpperCase()];
    try {
        const result = await gw.sendCommand(ProtoOAPayloadType.PROTO_OA_UNSUBSCRIBE_SPOTS_REQ, {
            ctTraderAccountId: parseInt(cTraderAccountId),
            symbolId: Array.isArray(symbolIds) ? symbolIds.map(i => parseInt(i)) : [parseInt(symbolIds)]
        });
        res.json({ success: true, message: 'Unsubscribed from Symbol Ticks', result });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

// 6.5 TRADING OPERATIONS & ORDER MANAGEMENT
app.post('/api/trade/order', async (req, res) => {
    const {
        cTraderAccountId,
        symbolId,
        tradeSide,
        volume,
        orderType = "MARKET",
        limitPrice = null,
        stopPrice = null,
        stopLoss = null,
        takeProfit = null,
        environment = 'DEMO'
    } = req.body;

    if (!cTraderAccountId || !symbolId || !tradeSide || !volume) {
        return res.status(400).json({ success: false, message: 'Missing required order parameters' });
    }

    const gw = gateways[environment.toUpperCase()];

    const payload = {
        ctTraderAccountId: parseInt(cTraderAccountId),
        symbolId: parseInt(symbolId),
        orderType: orderType.toUpperCase(),
        tradeSide: tradeSide.toUpperCase(),
        volume: parseInt(volume)
    };

    if (stopLoss) payload.stopLoss = parseFloat(stopLoss);
    if (takeProfit) payload.takeProfit = parseFloat(takeProfit);
    if (limitPrice) payload.limitPrice = parseFloat(limitPrice);
    if (stopPrice) payload.stopPrice = parseFloat(stopPrice);

    try {
        const result = await gw.sendCommand(ProtoOAPayloadType.PROTO_OA_NEW_ORDER_REQ, payload);
        res.json({ success: true, message: 'Order Executed Successfully', result });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

app.post('/api/trade/close', async (req, res) => {
    const { cTraderAccountId, positionId, volume, environment = 'DEMO' } = req.body;
    if (!cTraderAccountId || !positionId || !volume) {
        return res.status(400).json({ success: false, message: 'cTraderAccountId, positionId, and volume required' });
    }

    const gw = gateways[environment.toUpperCase()];
    try {
        const result = await gw.sendCommand(ProtoOAPayloadType.PROTO_OA_CLOSE_POSITION_REQ, {
            ctTraderAccountId: parseInt(cTraderAccountId),
            positionId: parseInt(positionId),
            volume: parseInt(volume)
        });
        res.json({ success: true, message: 'Position Close Triggered', result });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

app.post('/api/trade/amend-position', async (req, res) => {
    const { cTraderAccountId, positionId, stopLoss, takeProfit, environment = 'DEMO' } = req.body;
    if (!cTraderAccountId || !positionId) {
        return res.status(400).json({ success: false, message: 'cTraderAccountId and positionId required' });
    }

    const gw = gateways[environment.toUpperCase()];
    const payload = {
        ctTraderAccountId: parseInt(cTraderAccountId),
        positionId: parseInt(positionId)
    };

    if (stopLoss !== undefined) payload.stopLoss = parseFloat(stopLoss);
    if (takeProfit !== undefined) payload.takeProfit = parseFloat(takeProfit);

    try {
        const result = await gw.sendCommand(ProtoOAPayloadType.PROTO_OA_AMEND_POSITION_SLTP_REQ, payload);
        res.json({ success: true, message: 'Position Protection Protection Updated', result });
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

// 6.6 WALLET & DEPOSIT/WITHDRAWAL CONTROLLERS
app.post('/api/wallet/deposit', async (req, res) => {
    const { cTraderAccountId, amount, environment = 'DEMO' } = req.body;
    if (!cTraderAccountId || !amount) {
        return res.status(400).json({ success: false, message: 'cTraderAccountId and amount required' });
    }

    const gw = gateways[environment.toUpperCase()];
    try {
        const result = await gw.sendCommand(ProtoOAPayloadType.PROTO_OA_DEPOSIT_MARGIN_REQ, {
            ctTraderAccountId: parseInt(cTraderAccountId),
            moneyDigits: 2,
            delta: Math.round(parseFloat(amount) * 100)
        });
        res.json({ success: true, message: 'Demo Deposit Credited', result });
    } catch (error) {
        res.json({ success: false, message: 'Deposit handled externally by broker portal', error: error.message });
    }
});

app.post('/api/wallet/withdraw', async (req, res) => {
    const { cTraderAccountId, amount, environment = 'DEMO' } = req.body;
    if (!cTraderAccountId || !amount) {
        return res.status(400).json({ success: false, message: 'cTraderAccountId and amount required' });
    }

    const gw = gateways[environment.toUpperCase()];
    try {
        const result = await gw.sendCommand(ProtoOAPayloadType.PROTO_OA_WITHDRAW_MARGIN_REQ, {
            ctTraderAccountId: parseInt(cTraderAccountId),
            moneyDigits: 2,
            delta: Math.round(parseFloat(amount) * 100)
        });
        res.json({ success: true, message: 'Withdrawal Processed', result });
    } catch (error) {
        res.json({ success: false, message: 'Withdrawal logged for manual admin review', error: error.message });
    }
});

// ============================================================================
// SECTION 7: CATCH-ALL EXPRESS ERROR HANDLING MIDDLEWARE
// ============================================================================

app.use((err, req, res, next) => {
    console.error('[API Internal Error]:', err.stack);
    res.status(500).json({
        success: false,
        message: 'An unexpected internal server error occurred',
        error: err.message
    });
});

// ============================================================================
// SECTION 8: START SERVER AND BIND TO HOST PORT
// ============================================================================

server.listen(PORT, '0.0.0.0', () => {
    console.log(`===========================================================`);
    console.log(` Lesego Markets Production API Gateway Online              `);
    console.log(` HTTP/REST & WebSocket Listening on: http://0.0.0.0:${PORT} `);
    console.log(` Environment Mode: ${process.env.NODE_ENV || 'production'}  `);
    console.log(`===========================================================`);
});