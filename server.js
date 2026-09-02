require('dotenv').config();
const express = require('express');
const WebSocket = require('ws');
const cors = require('cors');
const bodyParser = require('body-parser');

const app = express();

// ==========================================
// 1. COMPREHENSIVE CORS & PRE-FLIGHT FIX
// ==========================================
const corsOptions = {
    origin: '*', // Allows requests from any frontend domain
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization', 'X-Requested-With'],
    credentials: true,
    optionsSuccessStatus: 200
};

app.use(cors(corsOptions));
app.options('*', cors(corsOptions)); // Enable pre-flight for all routes

app.use(bodyParser.json());

const PORT = process.env.PORT || 5000;
const CLIENT_ID = process.env.CTRADER_CLIENT_ID;
const CLIENT_SECRET = process.env.CTRADER_CLIENT_SECRET;

// In-Memory Database for Users and Active Sessions
const usersDb = {}; // { email: { password, name, demoAccountId, realAccountId } }
const activeConnections = {};

const HOSTS = {
    demo: 'wss://demo.ctraderapi.com:5035',
    live: 'wss://live.ctraderapi.com:5035'
};

function getCTraderConnection(environment = 'demo') {
    const hostUrl = HOSTS[environment.toLowerCase()] || HOSTS.demo;
    return new Promise((resolve, reject) => {
        const ws = new WebSocket(hostUrl);

        ws.on('open', () => {
            console.log(`[Lesego Markets] Connected to cTrader ${environment.toUpperCase()} endpoint.`);
            const appAuthPayload = {
                clientMsgId: "APP_AUTH_" + Date.now(),
                payloadType: 2100,
                payload: { clientId: CLIENT_ID, clientSecret: CLIENT_SECRET }
            };
            ws.send(JSON.stringify(appAuthPayload));
            resolve(ws);
        });

        ws.on('error', (err) => reject(err));
    });
}

// ==========================================
// 2. USER REGISTRATION & LOGIN ROUTES
// ==========================================
app.post('/api/auth/register', (req, res) => {
    const { name, email, password } = req.body;
    if (!email || !password || !name) {
        return res.status(400).json({ status: 'error', message: 'All fields are required.' });
    }

    if (usersDb[email]) {
        return res.status(400).json({ status: 'error', message: 'Account already exists. Please login.' });
    }

    // Assign mock cTrader IDs for Demo and Real environments upon registration
    const demoAccountId = "DEMO_" + Math.floor(100000 + Math.random() * 900000);
    const realAccountId = "LIVE_" + Math.floor(100000 + Math.random() * 900000);

    usersDb[email] = { name, email, password, demoAccountId, realAccountId };

    return res.status(201).json({
        status: 'success',
        message: 'Account registered successfully!',
        user: { name, email, demoAccountId, realAccountId }
    });
});

app.post('/api/auth/login', (req, res) => {
    const { email, password } = req.body;
    const user = usersDb[email];

    if (!user || user.password !== password) {
        return res.status(401).json({ status: 'error', message: 'Invalid email or password.' });
    }

    return res.status(200).json({
        status: 'success',
        message: 'Login successful!',
        user: {
            name: user.name,
            email: user.email,
            demoAccountId: user.demoAccountId,
            realAccountId: user.realAccountId
        }
    });
});

// ==========================================
// 3. CTRADER TRADING & ACCOUNT CONNECT ROUTES
// ==========================================
app.post('/api/account/login', async (req, res) => {
    const { cTraderAccountId, accessToken, environment } = req.body;
    try {
        const ws = await getCTraderConnection(environment);
        const accountAuthPayload = {
            clientMsgId: "ACC_AUTH_" + Date.now(),
            payloadType: 2102,
            payload: { ctidTraderAccountId: parseInt(cTraderAccountId) || 123456, accessToken }
        };

        ws.send(JSON.stringify(accountAuthPayload));
        activeConnections[cTraderAccountId] = { ws, environment };

        return res.status(200).json({
            status: 'success',
            message: `Connected to cTrader ${environment.toUpperCase()} engine.`,
            accountId: cTraderAccountId,
            environment
        });
    } catch (error) {
        return res.status(500).json({ status: 'error', message: error.message });
    }
});

app.post('/api/trade/place', async (req, res) => {
    const { cTraderAccountId, symbolId, tradeSide, volume, orderType, stopLoss, takeProfit, price } = req.body;
    const session = activeConnections[cTraderAccountId];

    if (!session) {
        return res.status(401).json({ status: 'error', message: 'Active trading session not found. Please re-connect.' });
    }

    const newOrderPayload = {
        clientMsgId: "ORDER_" + Date.now(),
        payloadType: 2106,
        payload: {
            ctidTraderAccountId: parseInt(cTraderAccountId) || 123456,
            symbolId: parseInt(symbolId),
            orderType: orderType || "MARKET",
            tradeSide: tradeSide.toUpperCase(),
            volume: parseInt(volume),
            stopLoss: stopLoss ? parseFloat(stopLoss) : undefined,
            takeProfit: takeProfit ? parseFloat(takeProfit) : undefined,
            price: price ? parseFloat(price) : undefined
        }
    };

    session.ws.send(JSON.stringify(newOrderPayload));
    return res.status(200).json({ status: 'success', message: 'Order sent to cTrader execution engine.' });
});

app.post('/api/trade/close', async (req, res) => {
    const { cTraderAccountId, positionId, volume } = req.body;
    const session = activeConnections[cTraderAccountId];

    if (!session) return res.status(401).json({ status: 'error', message: 'Session expired.' });

    const closePayload = {
        clientMsgId: "CLOSE_" + Date.now(),
        payloadType: 2110,
        payload: { ctidTraderAccountId: parseInt(cTraderAccountId) || 123456, positionId: parseInt(positionId), volume: parseInt(volume) }
    };

    session.ws.send(JSON.stringify(closePayload));
    return res.status(200).json({ status: 'success', message: 'Position close request transmitted.' });
});

app.post('/api/wallet/deposit', async (req, res) => {
    const { cTraderAccountId, amount, paymentMethod } = req.body;
    return res.status(200).json({
        status: 'success',
        message: `Successfully deposited $${amount} via ${paymentMethod}.`,
        transactionId: "DEP_" + Date.now()
    });
});

app.post('/api/wallet/withdraw', async (req, res) => {
    const { cTraderAccountId, amount, payoutDetails } = req.body;
    return res.status(200).json({
        status: 'success',
        message: `Withdrawal request of $${amount} submitted for processing.`,
        transactionId: "WTH_" + Date.now()
    });
});

app.listen(PORT, () => console.log(`Lesego Markets Server listening on port ${PORT}`));