/**
 * Lesego Markets - cTrader Open API Production Gateway
 * Standard: Open API v2 Protocol (WebSocket + OAuth 2.0)
 */

const express = require('express');
const cors = require('cors');
const axios = require('axios');
const WebSocket = require('ws');
const http = require('http');
require('dotenv').config();

// ==========================================
// 1. GLOBAL CONFIGURATION & CONSTANTS
// ==========================================
const PORT = process.env.PORT || 10000;
const CTRADER_CLIENT_ID = process.env.CTRADER_CLIENT_ID || "38384_wh6ecCD5h0tHjsNXc57f7a0f2aZeKUubeFlpkKDMpQqHn58H0m";
const CTRADER_CLIENT_SECRET = process.env.CTRADER_CLIENT_SECRET || "jkcXDForVeNulNasjMa1vnQKtZwbrOLjgH4GDLL3dkVWZVC0V4";
const CTRADER_REDIRECT_URI = process.env.CTRADER_REDIRECT_URI || "https://lesegomarkets.com/oauth/callback";

// cTrader Open API WebSocket Endpoints (Port 5035 for WebSockets)
const CTRADER_HOSTS = {
    DEMO: 'wss://demo.ctraderapi.com:5035',
    LIVE: 'wss://live.ctraderapi.com:5035'
};

// cTrader Open API ProtoPayloadType Identifiers
const PROTO_PAYLOAD_TYPE = {
    HEARTBEAT_EVENT: 51,
    PROTO_OA_APPLICATION_AUTH_REQ: 2100,
    PROTO_OA_APPLICATION_AUTH_RES: 2101,
    PROTO_OA_ACCOUNT_AUTH_REQ: 2102,
    PROTO_OA_ACCOUNT_AUTH_RES: 2103,
    PROTO_OA_NEW_ORDER_REQ: 2106,
    PROTO_OA_CLOSE_POSITION_REQ: 2107,
    PROTO_OA_CANCEL_ORDER_REQ: 2108,
    PROTO_OA_AMEND_POSITION_SLTP_REQ: 2109,
    PROTO_OA_SYMBOLS_LIST_REQ: 2114,
    PROTO_OA_SYMBOLS_LIST_RES: 2115,
    PROTO_OA_TRADER_REQ: 2121,
    PROTO_OA_TRADER_RES: 2122,
    PROTO_OA_RECONCILE_REQ: 2124,
    PROTO_OA_RECONCILE_RES: 2125,
    PROTO_OA_EXECUTION_EVENT: 2126,
    PROTO_OA_SUBSCRIBE_SPOTS_REQ: 2135,
    PROTO_OA_SPOT_EVENT: 2136,
    PROTO_OA_ERROR_RES: 2142,
    PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_REQ: 2149,
    PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_RES: 2150,
    PROTO_OA_DEPOSIT_MARGIN_REQ: 2154,
    PROTO_OA_WITHDRAW_MARGIN_REQ: 2155
};

// ==========================================
// 2. EXPRESS APP & SERVER INITIALIZATION
// ==========================================
const app = express();
const server = http.createServer(app);

// CORS Policy Configuration (Prevents Browser Blocking)
app.use(cors({
    origin: '*',
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization', 'X-Requested-With']
}));

app.use(express.json());

// Frontend WebSocket Server for broadcasting live ticks and events to web client
const wssFrontend = new WebSocket.Server({ noServer: true });

server.on('upgrade', (request, socket, head) => {
    wssFrontend.handleUpgrade(request, socket, head, (ws) => {
        wssFrontend.emit('connection', ws, request);
    });
});

// Active Active cTrader Connection Pool
const socketPool = {
    DEMO: null,
    LIVE: null
};

const clientSessions = new Map(); // Store user access tokens & connected accounts

// ==========================================
// 3. CTRADER WEBSOCKET CLIENT MANAGER
// ==========================================
class CTraderConnectionManager {
    constructor(environment = 'DEMO') {
        this.environment = environment;
        this.url = CTRADER_HOSTS[environment];
        this.ws = null;
        this.isAuthorized = false;
        this.pendingRequests = new Map();
        this.pingInterval = null;
    }

    connect() {
        return new Promise((resolve, reject) => {
            console.log(`[cTrader Gateway] Connecting to ${this.environment} at ${this.url}...`);
            this.ws = new WebSocket(this.url);

            this.ws.on('open', () => {
                console.log(`[cTrader Gateway] Connected to ${this.environment} WebSocket.`);
                this.startHeartbeat();
                this.authorizeApplication()
                    .then(() => {
                        this.isAuthorized = true;
                        resolve(true);
                    })
                    .catch(reject);
            });

            this.ws.on('message', (data) => {
                this.handleIncomingMessage(data);
            });

            this.ws.on('error', (err) => {
                console.error(`[cTrader Gateway] WebSocket Error (${this.environment}):`, err.message);
            });

            this.ws.on('close', () => {
                console.warn(`[cTrader Gateway] Connection lost to ${this.environment}. Reconnecting in 5s...`);
                this.isAuthorized = false;
                this.stopHeartbeat();
                setTimeout(() => this.connect(), 5000);
            });
        });
    }

    startHeartbeat() {
        this.pingInterval = setInterval(() => {
            if (this.ws && this.ws.readyState === WebSocket.OPEN) {
                this.sendPayload(PROTO_PAYLOAD_TYPE.HEARTBEAT_EVENT, {});
            }
        }, 10000);
    }

    stopHeartbeat() {
        if (this.pingInterval) clearInterval(this.pingInterval);
    }

    authorizeApplication() {
        console.log(`[cTrader Gateway] Authorizing App Credentials for ${this.environment}...`);
        const payload = {
            clientId: CTRADER_CLIENT_ID,
            clientSecret: CTRADER_CLIENT_SECRET
        };
        return this.sendRequest(PROTO_PAYLOAD_TYPE.PROTO_OA_APPLICATION_AUTH_REQ, payload);
    }

    sendPayload(payloadType, msgData, clientMsgId = null) {
        if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
            throw new Error('cTrader WebSocket connection is not open');
        }

        const jsonWrapper = JSON.stringify({
            payloadType: payloadType,
            payload: msgData,
            clientMsgId: clientMsgId || `msg_${Date.now()}_${Math.random().toString(36).substring(2, 7)}`
        });

        this.ws.send(jsonWrapper);
    }

    sendRequest(payloadType, msgData) {
        return new Promise((resolve, reject) => {
            const clientMsgId = `req_${Date.now()}_${Math.random().toString(36).substring(2, 7)}`;
            
            const timeout = setTimeout(() => {
                this.pendingRequests.delete(clientMsgId);
                reject(new Error(`Request timeout for PayloadType: ${payloadType}`));
            }, 15000);

            this.pendingRequests.set(clientMsgId, { resolve, reject, timeout });

            try {
                this.sendPayload(payloadType, msgData, clientMsgId);
            } catch (err) {
                this.pendingRequests.delete(clientMsgId);
                clearTimeout(timeout);
                reject(err);
            }
        });
    }

    handleIncomingMessage(data) {
        try {
            const parsed = JSON.parse(data.toString());
            const { payloadType, payload, clientMsgId } = parsed;

            // Handle Heartbeats
            if (payloadType === PROTO_PAYLOAD_TYPE.HEARTBEAT_EVENT) return;

            // Forward execution events and market ticks to web clients
            if (payloadType === PROTO_PAYLOAD_TYPE.PROTO_OA_SPOT_EVENT || payloadType === PROTO_PAYLOAD_TYPE.PROTO_OA_EXECUTION_EVENT) {
                broadcastToFrontend({ payloadType, payload });
            }

            // Resolve pending requests
            if (clientMsgId && this.pendingRequests.has(clientMsgId)) {
                const { resolve, reject, timeout } = this.pendingRequests.get(clientMsgId);
                clearTimeout(timeout);
                this.pendingRequests.delete(clientMsgId);

                if (payloadType === PROTO_PAYLOAD_TYPE.PROTO_OA_ERROR_RES) {
                    reject(new Error(payload.description || 'cTrader API Returned Error'));
                } else {
                    resolve(payload);
                }
            }
        } catch (err) {
            console.error('[cTrader Gateway] Failed to parse WebSocket frame:', err.message);
        }
    }
}

// Initialize Connection Pool
socketPool.DEMO = new CTraderConnectionManager('DEMO');
socketPool.LIVE = new CTraderConnectionManager('LIVE');

Promise.all([
    socketPool.DEMO.connect().catch(e => console.error("DEMO Init Error:", e.message)),
    socketPool.LIVE.connect().catch(e => console.error("LIVE Init Error:", e.message))
]);

// ==========================================
// 4. FRONTEND WEBSOCKET BROADCASTING
// ==========================================
function broadcastToFrontend(message) {
    const jsonStr = JSON.stringify(message);
    wssFrontend.clients.forEach(client => {
        if (client.readyState === WebSocket.OPEN) {
            client.send(jsonStr);
        }
    });
}

wssFrontend.on('connection', (ws) => {
    console.log('[Frontend WS] New Web Client Connected to Live Stream.');
    ws.send(JSON.stringify({ status: 'CONNECTED', message: 'Lesego Markets Realtime Gateway Active' }));
});

// ==========================================
// 5. REST API ROUTES (OAUTH & TRADING)
// ==========================================

// Health Check Endpoint (Required for Render Deployment)
app.get('/health', (req, res) => {
    res.status(200).json({
        status: 'HEALTHY',
        service: 'Lesego Markets cTrader Server',
        demoSocket: socketPool.DEMO.isAuthorized ? 'CONNECTED' : 'DISCONNECTED',
        liveSocket: socketPool.LIVE.isAuthorized ? 'CONNECTED' : 'DISCONNECTED',
        timestamp: new Date()
    });
});

app.get('/', (req, res) => {
    res.status(200).send('Lesego Markets cTrader Open API Backend Engine Active.');
});

// ------------------------------------------
// AUTHENTICATION (cTID OAuth 2.0 Integration)
// ------------------------------------------

// Fetch cTrader OAuth Direct Authorization URL
app.get('/api/auth/login-url', (req, res) => {
    const authUrl = `https://openapi.ctrader.com/apps/auth?client_id=${CTRADER_CLIENT_ID}&redirect_uri=${encodeURIComponent(CTRADER_REDIRECT_URI)}&scope=trading`;
    res.json({ success: true, authUrl });
});

// Exchange Authorization Code for cTrader Tokens
app.post('/api/auth/token', async (req, res) => {
    const { code } = req.body;
    if (!code) {
        return res.status(400).json({ success: false, message: 'Authorization code is required' });
    }

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

        const tokenData = response.data; // access_token, refresh_token, accessTokenExpirations
        res.json({ success: true, auth: tokenData });
    } catch (error) {
        console.error('[OAuth Token Error]:', error.response?.data || error.message);
        res.status(500).json({
            success: false,
            message: 'cTrader Authentication Failed',
            error: error.response?.data || error.message
        });
    }
});

// Refresh Expired cTrader Token
app.post('/api/auth/refresh', async (req, res) => {
    const { refreshToken } = req.body;
    if (!refreshToken) return res.status(400).json({ success: false, message: 'Refresh token required' });

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

// ------------------------------------------
// ACCOUNTS MANAGEMENT
// ------------------------------------------

// Fetch all trading accounts associated with access token
app.post('/api/accounts/list', async (req, res) => {
    const { accessToken, environment = 'DEMO' } = req.body;
    if (!accessToken) return res.status(400).json({ success: false, message: 'Access token required' });

    const manager = socketPool[environment.toUpperCase()];
    if (!manager || !manager.isAuthorized) {
        return res.status(503).json({ success: false, message: 'cTrader Gateway connection unavailable' });
    }

    try {
        const result = await manager.sendRequest(PROTO_PAYLOAD_TYPE.PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_REQ, {
            accessToken: accessToken
        });
        res.json({ success: true, accounts: result.ctaTraderAccount });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

// Authorize Account for Trading Session
app.post('/api/accounts/authenticate', async (req, res) => {
    const { accessToken, cTraderAccountId, environment = 'DEMO' } = req.body;
    if (!accessToken || !cTraderAccountId) {
        return res.status(400).json({ success: false, message: 'Access Token and Account ID required' });
    }

    const manager = socketPool[environment.toUpperCase()];
    try {
        const result = await manager.sendRequest(PROTO_PAYLOAD_TYPE.PROTO_OA_ACCOUNT_AUTH_REQ, {
            accessToken: accessToken,
            ctraderAccountId: parseInt(cTraderAccountId)
        });
        res.json({ success: true, message: 'Account Authorized Successfully', details: result });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

// Get Detailed Balance / Margin / Equity Info
app.get('/api/accounts/trader-info', async (req, res) => {
    const { cTraderAccountId, environment = 'DEMO' } = req.query;
    if (!cTraderAccountId) return res.status(400).json({ success: false, message: 'cTraderAccountId is required' });

    const manager = socketPool[environment.toUpperCase()];
    try {
        const result = await manager.sendRequest(PROTO_PAYLOAD_TYPE.PROTO_OA_TRADER_REQ, {
            ctraderAccountId: parseInt(cTraderAccountId)
        });
        res.json({ success: true, trader: result.trader });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

// ------------------------------------------
// TRADING & ORDER MANAGEMENT
// ------------------------------------------

// Place Market / Limit Order
app.post('/api/trade/order', async (req, res) => {
    const {
        cTraderAccountId,
        symbolId,
        tradeSide, // "BUY" or "SELL"
        volume, // In Cents/Units (e.g., 100000 = 1 Lot)
        orderType = "MARKET", // "MARKET", "LIMIT", "STOP"
        limitPrice = null,
        stopLoss = null,
        takeProfit = null,
        environment = 'DEMO'
    } = req.body;

    if (!cTraderAccountId || !symbolId || !tradeSide || !volume) {
        return res.status(400).json({ success: false, message: 'Missing order parameters' });
    }

    const manager = socketPool[environment.toUpperCase()];

    const payload = {
        ctraderAccountId: parseInt(cTraderAccountId),
        symbolId: parseInt(symbolId),
        orderType: orderType,
        tradeSide: tradeSide.toUpperCase(),
        volume: parseInt(volume),
        stopLoss: stopLoss ? parseFloat(stopLoss) : undefined,
        takeProfit: takeProfit ? parseFloat(takeProfit) : undefined
    };

    if (orderType === "LIMIT" && limitPrice) {
        payload.limitPrice = parseFloat(limitPrice);
    }

    try {
        const result = await manager.sendRequest(PROTO_PAYLOAD_TYPE.PROTO_OA_NEW_ORDER_REQ, payload);
        res.json({ success: true, message: 'Order Executed', result });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

// Close Active Position
app.post('/api/trade/close', async (req, res) => {
    const { cTraderAccountId, positionId, volume, environment = 'DEMO' } = req.body;
    if (!cTraderAccountId || !positionId || !volume) {
        return res.status(400).json({ success: false, message: 'AccountId, PositionId, and Volume required' });
    }

    const manager = socketPool[environment.toUpperCase()];
    try {
        const result = await manager.sendRequest(PROTO_PAYLOAD_TYPE.PROTO_OA_CLOSE_POSITION_REQ, {
            ctraderAccountId: parseInt(cTraderAccountId),
            positionId: parseInt(positionId),
            volume: parseInt(volume)
        });
        res.json({ success: true, message: 'Position Closed', result });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

// Get Active Open Positions and Orders
app.get('/api/trade/positions', async (req, res) => {
    const { cTraderAccountId, environment = 'DEMO' } = req.query;
    if (!cTraderAccountId) return res.status(400).json({ success: false, message: 'cTraderAccountId is required' });

    const manager = socketPool[environment.toUpperCase()];
    try {
        const result = await manager.sendRequest(PROTO_PAYLOAD_TYPE.PROTO_OA_RECONCILE_REQ, {
            ctraderAccountId: parseInt(cTraderAccountId)
        });
        res.json({ success: true, positions: result.position, orders: result.order });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

// ------------------------------------------
// MARKET DATA & SYMBOLS
// ------------------------------------------

// Get Available Symbols
app.get('/api/market/symbols', async (req, res) => {
    const { cTraderAccountId, environment = 'DEMO' } = req.query;
    if (!cTraderAccountId) return res.status(400).json({ success: false, message: 'cTraderAccountId required' });

    const manager = socketPool[environment.toUpperCase()];
    try {
        const result = await manager.sendRequest(PROTO_PAYLOAD_TYPE.PROTO_OA_SYMBOLS_LIST_REQ, {
            ctraderAccountId: parseInt(cTraderAccountId)
        });
        res.json({ success: true, symbols: result.symbol });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

// Subscribe to Realtime Symbol Prices (Ticks)
app.post('/api/market/subscribe', async (req, res) => {
    const { cTraderAccountId, symbolIds, environment = 'DEMO' } = req.body; // Array of symbol IDs
    if (!cTraderAccountId || !symbolIds) {
        return res.status(400).json({ success: false, message: 'AccountId and SymbolIds required' });
    }

    const manager = socketPool[environment.toUpperCase()];
    try {
        const result = await manager.sendRequest(PROTO_PAYLOAD_TYPE.PROTO_OA_SUBSCRIBE_SPOTS_REQ, {
            ctraderAccountId: parseInt(cTraderAccountId),
            symbolId: Array.isArray(symbolIds) ? symbolIds.map(i => parseInt(i)) : [parseInt(symbolIds)]
        });
        res.json({ success: true, message: 'Subscribed to Market Ticks', result });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

// ------------------------------------------
// DEPOSIT & WITHDRAWAL GATEWAYS
// ------------------------------------------

// Deposit Funds Request
app.post('/api/wallet/deposit', async (req, res) => {
    const { cTraderAccountId, amount, environment = 'DEMO' } = req.body;
    if (!cTraderAccountId || !amount) {
        return res.status(400).json({ success: false, message: 'AccountId and Amount required' });
    }

    const manager = socketPool[environment.toUpperCase()];
    try {
        // Forward margin deposit request to cTrader API if permitted
        const result = await manager.sendRequest(PROTO_PAYLOAD_TYPE.PROTO_OA_DEPOSIT_MARGIN_REQ, {
            ctraderAccountId: parseInt(cTraderAccountId),
            moneyDigits: 2,
            delta: parseInt(amount * 100)
        });
        res.json({ success: true, message: 'Deposit Processed', result });
    } catch (error) {
        // Return standard feedback if broker handles deposits externally
        res.json({ 
            success: false, 
            message: 'Direct API deposit depends on broker privileges. Redirecting to payment portal...',
            error: error.message 
        });
    }
});

// Withdraw Funds Request
app.post('/api/wallet/withdraw', async (req, res) => {
    const { cTraderAccountId, amount, environment = 'DEMO' } = req.body;
    if (!cTraderAccountId || !amount) {
        return res.status(400).json({ success: false, message: 'AccountId and Amount required' });
    }

    const manager = socketPool[environment.toUpperCase()];
    try {
        const result = await manager.sendRequest(PROTO_PAYLOAD_TYPE.PROTO_OA_WITHDRAW_MARGIN_REQ, {
            ctraderAccountId: parseInt(cTraderAccountId),
            moneyDigits: 2,
            delta: parseInt(amount * 100)
        });
        res.json({ success: true, message: 'Withdrawal Executed', result });
    } catch (error) {
        res.json({ 
            success: false, 
            message: 'Withdrawal requested. Awaiting broker approval.',
            error: error.message 
        });
    }
});

// ==========================================
// 6. SERVER BINDING (RENDER READY)
// ==========================================
server.listen(PORT, '0.0.0.0', () => {
    console.log(`====================================================`);
    console.log(` Lesego Markets cTrader API Server Active          `);
    console.log(` Port: ${PORT}                                      `);
    console.log(` Bind Address: 0.0.0.0                             `);
    console.log(`====================================================`);
});