/**
 * Lesego Markets - cTrader Open API Backend Engine
 * Fixed: Official Protobuf SDK framing & Process Crash Guards
 */

const express = require('express');
const cors = require('cors');
const axios = require('axios');
const http = require('http');
const { CTraderConnection, ProtoOAApplicationAuthReq, ProtoOAPayloadType } = require('@spotware/open-api-sdk');
require('dotenv').config();

// --- 1. CRASH PREVENTION GUARDS (Prevents Node.js exit on Render) ---
process.on('unhandledRejection', (reason, promise) => {
    console.error('[Crash Guard] Unhandled Rejection:', reason?.message || reason);
});

process.on('uncaughtException', (err) => {
    console.error('[Crash Guard] Uncaught Exception:', err.message);
});

// --- 2. CONFIGURATION & CREDENTIALS ---
const PORT = process.env.PORT || 10000;
const CTRADER_CLIENT_ID = process.env.CTRADER_CLIENT_ID || "38384_wh6ecCD5h0tHjsNXc57f7a0f2aZeKUubeFlpkKDMpQqHn58H0m";
const CTRADER_CLIENT_SECRET = process.env.CTRADER_CLIENT_SECRET || "jkcXDForVeNulNasjMa1vnQKtZwbrOLjgH4GDLL3dkVWZVC0V4";
const CTRADER_REDIRECT_URI = process.env.CTRADER_REDIRECT_URI || "https://lesego.onrender.com";

const ENDPOINTS = {
    DEMO: { host: 'demo.ctraderapi.com', port: 5035 },
    LIVE: { host: 'live.ctraderapi.com', port: 5035 }
};

const app = express();
const server = http.createServer(app);

app.use(cors({ origin: '*', methods: ['GET', 'POST', 'OPTIONS'] }));
app.use(express.json());

// --- 3. OFFICIAL CTRADER PROTOBUF WEBSOCKET CONNECTIONS ---
const connections = {
    DEMO: { connection: null, isAuthorized: false },
    LIVE: { connection: null, isAuthorized: false }
};

function initCTraderSocket(env) {
    const { host, port } = ENDPOINTS[env];
    console.log(`[cTrader Gateway] Connecting to ${env} at ${host}:${port}...`);

    const connection = new CTraderConnection({ host, port });
    connections[env].connection = connection;

    connection.on('open', () => {
        console.log(`[cTrader Gateway] Connected to ${env} Protobuf WebSocket.`);
        authorizeApp(env);
    });

    connection.on('error', (err) => {
        console.error(`[cTrader Gateway] ${env} Socket Error:`, err.message);
    });

    connection.on('close', () => {
        console.warn(`[cTrader Gateway] Connection lost to ${env}. Reconnecting in 5s...`);
        connections[env].isAuthorized = false;
        setTimeout(() => initCTraderSocket(env), 5000);
    });

    connection.connect();
}

async function authorizeApp(env) {
    const conn = connections[env].connection;
    console.log(`[cTrader Gateway] Authorizing App Credentials for ${env}...`);

    try {
        await conn.sendCommand(ProtoOAPayloadType.PROTO_OA_APPLICATION_AUTH_REQ, {
            clientId: CTRADER_CLIENT_ID,
            clientSecret: CTRADER_CLIENT_SECRET
        });
        connections[env].isAuthorized = true;
        console.log(`[cTrader Gateway] Application Authorized Successfully on ${env}!`);
    } catch (err) {
        console.error(`[cTrader Gateway] App Authorization Failed for ${env}:`, err.message);
    }
}

// Initialize sockets safely
initCTraderSocket('DEMO');
initCTraderSocket('LIVE');

// --- 4. EXPRESS REST API ENDPOINTS ---

// Health Check for Render Deployment
app.get('/health', (req, res) => {
    res.status(200).json({
        status: 'HEALTHY',
        service: 'Lesego Markets cTrader Bridge',
        demoAuthorized: connections.DEMO.isAuthorized,
        liveAuthorized: connections.LIVE.isAuthorized,
        timestamp: new Date()
    });
});

app.get('/', (req, res) => {
    res.status(200).send('Lesego Markets cTrader API Gateway Active.');
});

// Get cTrader OAuth Authorize URL
app.get('/api/auth/login-url', (req, res) => {
    const authUrl = `https://openapi.ctrader.com/apps/auth?client_id=${CTRADER_CLIENT_ID}&redirect_uri=${encodeURIComponent(CTRADER_REDIRECT_URI)}&scope=trading`;
    res.json({ success: true, authUrl });
});

// Exchange OAuth Code for Token
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
        res.status(500).json({ success: false, message: error.response?.data || error.message });
    }
});

// Fetch Accounts via Access Token
app.post('/api/accounts/list', async (req, res) => {
    const { accessToken, environment = 'DEMO' } = req.body;
    const envKey = environment.toUpperCase();
    const connObj = connections[envKey];

    if (!connObj || !connObj.isAuthorized) {
        return res.status(503).json({ success: false, message: `${envKey} cTrader Gateway is connecting...` });
    }

    try {
        const result = await connObj.connection.sendCommand(
            ProtoOAPayloadType.PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_REQ,
            { accessToken }
        );
        res.json({ success: true, accounts: result.ctaTraderAccount });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

// Authenticate Individual Trading Account
app.post('/api/accounts/authenticate', async (req, res) => {
    const { accessToken, cTraderAccountId, environment = 'DEMO' } = req.body;
    const envKey = environment.toUpperCase();
    const connObj = connections[envKey];

    try {
        const result = await connObj.connection.sendCommand(
            ProtoOAPayloadType.PROTO_OA_ACCOUNT_AUTH_REQ,
            { accessToken, ctTraderAccountId: parseInt(cTraderAccountId) }
        );
        res.json({ success: true, details: result });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

// Start Server on Render
server.listen(PORT, '0.0.0.0', () => {
    console.log(`Lesego Markets Server listening on port ${PORT}`);
});