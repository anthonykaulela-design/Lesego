/**
 * Lesego Markets - cTrader Open API Engine
 * Standard: Official Protobuf SDK (@spotware/open-api-sdk)
 * Host: Render (0.0.0.0:10000)
 */

const express = require('express');
const cors = require('cors');
const axios = require('axios');
const http = require('http');
const WebSocket = require('ws');
const { CTraderConnection, ProtoOAPayloadType } = require('@spotware/open-api-sdk');
require('dotenv').config();

// ============================================================================
// 1. GLOBAL UNCAUGHT ERROR & CRASH PREVENTION GUARDS
// ============================================================================
process.on('unhandledRejection', (reason, promise) => {
    console.error('[Crash Guard] Caught Unhandled Rejection:', reason?.message || reason);
});

process.on('uncaughtException', (err) => {
    console.error('[Crash Guard] Caught Uncaught Exception:', err.message);
});

// ============================================================================
// 2. ENVIRONMENT & API CONFIGURATION
// ============================================================================
const PORT = process.env.PORT || 10000;
const CTRADER_CLIENT_ID = process.env.CTRADER_CLIENT_ID || "38384_wh6ecCD5h0tHjsNXc57f7a0f2aZeKUubeFlpkKDMpQqHn58H0m";
const CTRADER_CLIENT_SECRET = process.env.CTRADER_CLIENT_SECRET || "jkcXDForVeNulNasjMa1vnQKtZwbrOLjgH4GDLL3dkVWZVC0V4";
const CTRADER_REDIRECT_URI = process.env.CTRADER_REDIRECT_URI || "https://lesego.onrender.com";

const CTRADER_HOSTS = {
    DEMO: { host: 'demo.ctraderapi.com', port: 5035 },
    LIVE: { host: 'live.ctraderapi.com', port: 5035 }
};

// ============================================================================
// 3. SERVER & EXPRESS INITIALIZATION
// ============================================================================
const app = express();
const server = http.createServer(app);

app.use(cors({
    origin: '*',
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization', 'X-Requested-With']
}));

app.use(express.json());

// Frontend WebSocket Server for streaming ticks to web clients
const wssFrontend = new WebSocket.Server({ noServer: true });

server.on('upgrade', (request, socket, head) => {
    wssFrontend.handleUpgrade(request, socket, head, (ws) => {
        wssFrontend.emit('connection', ws, request);
    });
});

function broadcastToFrontend(data) {
    const message = JSON.stringify(data);
    wssFrontend.clients.forEach((client) => {
        if (client.readyState === WebSocket.OPEN) {
            client.send(message);
        }
    });
}

wssFrontend.on('connection', (ws) => {
    console.log('[Frontend WS] Web Client Connected to Live Broadcast.');
    ws.send(JSON.stringify({ type: 'SYSTEM', message: 'Connected to Lesego Markets Engine' }));
});

// ============================================================================
// 4. PROTOBUF CTRADER API GATEWAY CLASS
// ============================================================================
class CTraderProtobufGateway {
    constructor(environment = 'DEMO') {
        this.environment = environment;
        this.config = CTRADER_HOSTS[environment];
        this.connection = null;
        this.isAuthorized = false;
        this.isConnecting = false;
        this.pingInterval = null;
    }

    connect() {
        if (this.isConnecting) return;
        this.isConnecting = true;

        console.log(`[cTrader Gateway] Initializing ${this.environment} Protobuf Stream at ${this.config.host}:${this.config.port}...`);

        try {
            this.connection = new CTraderConnection({
                host: this.config.host,
                port: this.config.port
            });

            this.connection.on('open', () => {
                console.log(`[cTrader Gateway] ${this.environment} Protobuf Socket Connected.`);
                this.isConnecting = false;
                this.startHeartbeat();
                this.authorizeApplication().catch((err) => {
                    console.error(`[cTrader Gateway] ${this.environment} App Auth Error:`, err.message);
                });
            });

            this.connection.on('data', (data) => {
                this.handleIncomingFrame(data);
            });

            this.connection.on('error', (err) => {
                console.error(`[cTrader Gateway] ${this.environment} Socket Error:`, err.message);
                this.isConnecting = false;
            });

            this.connection.on('close', () => {
                console.warn(`[cTrader Gateway] Connection lost to ${this.environment}. Retrying in 5s...`);
                this.isAuthorized = false;
                this.isConnecting = false;
                this.stopHeartbeat();
                setTimeout(() => this.connect(), 5000);
            });

            this.connection.connect();
        } catch (err) {
            console.error(`[cTrader Gateway] Setup Failed for ${this.environment}:`, err.message);
            this.isConnecting = false;
            setTimeout(() => this.connect(), 5000);
        }
    }

    startHeartbeat() {
        this.stopHeartbeat();
        this.pingInterval = setInterval(() => {
            if (this.connection) {
                this.sendPayload(ProtoOAPayloadType.PROTO_OA_HEARTBEAT_EVENT, {}).catch(() => {});
            }
        }, 10000);
    }

    stopHeartbeat() {
        if (this.pingInterval) clearInterval(this.pingInterval);
    }

    async authorizeApplication() {
        console.log(`[cTrader Gateway] Authorizing App Credentials for ${this.environment}...`);
        const response = await this.sendPayload(ProtoOAPayloadType.PROTO_OA_APPLICATION_AUTH_REQ, {
            clientId: CTRADER_CLIENT_ID,
            clientSecret: CTRADER_CLIENT_SECRET
        });
        this.isAuthorized = true;
        console.log(`[cTrader Gateway] Application Authorized Successfully on ${this.environment}!`);
        return response;
    }

    sendPayload(payloadType, payload = {}) {
        return new Promise((resolve, reject) => {
            if (!this.connection) {
                return reject(new Error(`${this.environment} connection not available`));
            }

            const timeout = setTimeout(() => {
                reject(new Error(`cTrader Request Timeout for PayloadType: ${payloadType}`));
            }, 12000);

            this.connection.sendCommand(payloadType, payload)
                .then((res) => {
                    clearTimeout(timeout);
                    resolve(res);
                })
                .catch((err) => {
                    clearTimeout(timeout);
                    reject(err);
                });
        });
    }

    handleIncomingFrame(data) {
        try {
            if (data.payloadType === ProtoOAPayloadType.PROTO_OA_SPOT_EVENT || 
                data.payloadType === ProtoOAPayloadType.PROTO_OA_EXECUTION_EVENT) {
                broadcastToFrontend(data);
            }
        } catch (err) {
            console.error('[cTrader Gateway] Frame Broadcast Error:', err.message);
        }
    }
}

// Active Dual Stream Instances
const gateways = {
    DEMO: new CTraderProtobufGateway('DEMO'),
    LIVE: new CTraderProtobufGateway('LIVE')
};

gateways.DEMO.connect();
gateways.LIVE.connect();

// ============================================================================
// 5. REST API ROUTING ENGINE
// ============================================================================

// Render Health Check Endpoint
app.get('/health', (req, res) => {
    res.status(200).json({
        status: 'HEALTHY',
        service: 'Lesego Markets cTrader Gateway',
        demoStatus: gateways.DEMO.isAuthorized ? 'CONNECTED' : 'CONNECTING',
        liveStatus: gateways.LIVE.isAuthorized ? 'CONNECTED' : 'CONNECTING',
        timestamp: new Date()
    });
});

app.get('/', (req, res) => {
    res.status(200).send('Lesego Markets cTrader Open API Engine Online.');
});

// OAuth Login URL Generator
app.get('/api/auth/login-url', (req, res) => {
    const authUrl = `https://openapi.ctrader.com/apps/auth?client_id=${CTRADER_CLIENT_ID}&redirect_uri=${encodeURIComponent(CTRADER_REDIRECT_URI)}&scope=trading`;
    res.json({ success: true, authUrl });
});

// OAuth Code Exchange
app.post('/api/auth/token', async (req, res) => {
    const { code } = req.body;
    if (!code) return res.status(400).json({ success: false, message: 'Authorization code required' });

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
            message: 'cTrader Authentication Failed',
            error: error.response?.data || error.message
        });
    }
});

// Refresh Token
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

// Fetch Trading Accounts
app.post('/api/accounts/list', async (req, res) => {
    const { accessToken, environment = 'DEMO' } = req.body;
    if (!accessToken) return res.status(400).json({ success: false, message: 'Access token required' });

    const gw = gateways[environment.toUpperCase()];
    if (!gw) return res.status(400).json({ success: false, message: 'Invalid environment' });

    try {
        const result = await gw.sendPayload(
            ProtoOAPayloadType.PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_REQ,
            { accessToken }
        );
        res.json({ success: true, accounts: result.ctaTraderAccount });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

// Authenticate Account
app.post('/api/accounts/authenticate', async (req, res) => {
    const { accessToken, cTraderAccountId, environment = 'DEMO' } = req.body;
    if (!accessToken || !cTraderAccountId) {
        return res.status(400).json({ success: false, message: 'AccessToken and AccountID required' });
    }

    const gw = gateways[environment.toUpperCase()];
    try {
        const result = await gw.sendPayload(
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

// Fetch Account Info
app.get('/api/accounts/trader-info', async (req, res) => {
    const { cTraderAccountId, environment = 'DEMO' } = req.query;
    if (!cTraderAccountId) return res.status(400).json({ success: false, message: 'Account ID required' });

    const gw = gateways[environment.toUpperCase()];
    try {
        const result = await gw.sendPayload(
            ProtoOAPayloadType.PROTO_OA_TRADER_REQ,
            { ctTraderAccountId: parseInt(cTraderAccountId) }
        );
        res.json({ success: true, trader: result.trader });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

// Execute Orders
app.post('/api/trade/order', async (req, res) => {
    const {
        cTraderAccountId,
        symbolId,
        tradeSide,
        volume,
        orderType = "MARKET",
        limitPrice = null,
        stopLoss = null,
        takeProfit = null,
        environment = 'DEMO'
    } = req.body;

    if (!cTraderAccountId || !symbolId || !tradeSide || !volume) {
        return res.status(400).json({ success: false, message: 'Missing order parameters' });
    }

    const gw = gateways[environment.toUpperCase()];

    const payload = {
        ctTraderAccountId: parseInt(cTraderAccountId),
        symbolId: parseInt(symbolId),
        orderType: orderType,
        tradeSide: tradeSide.toUpperCase(),
        volume: parseInt(volume)
    };

    if (stopLoss) payload.stopLoss = parseFloat(stopLoss);
    if (takeProfit) payload.takeProfit = parseFloat(takeProfit);
    if (orderType === "LIMIT" && limitPrice) payload.limitPrice = parseFloat(limitPrice);

    try {
        const result = await gw.sendPayload(ProtoOAPayloadType.PROTO_OA_NEW_ORDER_REQ, payload);
        res.json({ success: true, message: 'Order Processed', result });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

// Close Position
app.post('/api/trade/close', async (req, res) => {
    const { cTraderAccountId, positionId, volume, environment = 'DEMO' } = req.body;
    if (!cTraderAccountId || !positionId || !volume) {
        return res.status(400).json({ success: false, message: 'AccountId, PositionId, and Volume required' });
    }

    const gw = gateways[environment.toUpperCase()];
    try {
        const result = await gw.sendPayload(ProtoOAPayloadType.PROTO_OA_CLOSE_POSITION_REQ, {
            ctTraderAccountId: parseInt(cTraderAccountId),
            positionId: parseInt(positionId),
            volume: parseInt(volume)
        });
        res.json({ success: true, message: 'Position Closed', result });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

// Reconcile Positions
app.get('/api/trade/positions', async (req, res) => {
    const { cTraderAccountId, environment = 'DEMO' } = req.query;
    if (!cTraderAccountId) return res.status(400).json({ success: false, message: 'Account ID required' });

    const gw = gateways[environment.toUpperCase()];
    try {
        const result = await gw.sendPayload(ProtoOAPayloadType.PROTO_OA_RECONCILE_REQ, {
            ctTraderAccountId: parseInt(cTraderAccountId)
        });
        res.json({ success: true, positions: result.position, orders: result.order });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

// Market Symbols List
app.get('/api/market/symbols', async (req, res) => {
    const { cTraderAccountId, environment = 'DEMO' } = req.query;
    if (!cTraderAccountId) return res.status(400).json({ success: false, message: 'Account ID required' });

    const gw = gateways[environment.toUpperCase()];
    try {
        const result = await gw.sendPayload(ProtoOAPayloadType.PROTO_OA_SYMBOLS_LIST_REQ, {
            ctTraderAccountId: parseInt(cTraderAccountId)
        });
        res.json({ success: true, symbols: result.symbol });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

// Subscribe to Market Ticks
app.post('/api/market/subscribe', async (req, res) => {
    const { cTraderAccountId, symbolIds, environment = 'DEMO' } = req.body;
    if (!cTraderAccountId || !symbolIds) {
        return res.status(400).json({ success: false, message: 'AccountId and SymbolIds required' });
    }

    const gw = gateways[environment.toUpperCase()];
    try {
        const result = await gw.sendPayload(ProtoOAPayloadType.PROTO_OA_SUBSCRIBE_SPOTS_REQ, {
            ctTraderAccountId: parseInt(cTraderAccountId),
            symbolId: Array.isArray(symbolIds) ? symbolIds.map(i => parseInt(i)) : [parseInt(symbolIds)]
        });
        res.json({ success: true, message: 'Subscribed to Live Ticks', result });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

// Deposits
app.post('/api/wallet/deposit', async (req, res) => {
    const { cTraderAccountId, amount, environment = 'DEMO' } = req.body;
    if (!cTraderAccountId || !amount) {
        return res.status(400).json({ success: false, message: 'AccountId and Amount required' });
    }

    const gw = gateways[environment.toUpperCase()];
    try {
        const result = await gw.sendPayload(ProtoOAPayloadType.PROTO_OA_DEPOSIT_MARGIN_REQ, {
            ctTraderAccountId: parseInt(cTraderAccountId),
            moneyDigits: 2,
            delta: parseInt(amount * 100)
        });
        res.json({ success: true, message: 'Deposit Applied', result });
    } catch (error) {
        res.json({ success: false, message: 'Broker handling deposit externally', error: error.message });
    }
});

// Withdrawals
app.post('/api/wallet/withdraw', async (req, res) => {
    const { cTraderAccountId, amount, environment = 'DEMO' } = req.body;
    if (!cTraderAccountId || !amount) {
        return res.status(400).json({ success: false, message: 'AccountId and Amount required' });
    }

    const gw = gateways[environment.toUpperCase()];
    try {
        const result = await gw.sendPayload(ProtoOAPayloadType.PROTO_OA_WITHDRAW_MARGIN_REQ, {
            ctTraderAccountId: parseInt(cTraderAccountId),
            moneyDigits: 2,
            delta: parseInt(amount * 100)
        });
        res.json({ success: true, message: 'Withdrawal Submitted', result });
    } catch (error) {
        res.json({ success: false, message: 'Withdrawal logged for review', error: error.message });
    }
});

// ============================================================================
// 6. BIND SERVER TO PORT
// ============================================================================
server.listen(PORT, '0.0.0.0', () => {
    console.log(`====================================================`);
    console.log(` Lesego Markets Production Engine Online           `);
    console.log(` Listening on 0.0.0.0:${PORT}                      `);
    console.log(`====================================================`);
});