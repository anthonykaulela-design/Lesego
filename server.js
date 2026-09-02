const express = require('express');
const cors = require('cors');
const jwt = require('jsonwebtoken');
const bcrypt = require('bcryptjs');
const WebSocket = require('ws');
require('dotenv').config();

const app = express();

// --- 1. CORS & Middleware Configuration ---
app.use(cors({
    origin: '*', // Allows all frontend domains to connect without CORS issues
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization']
}));
app.use(express.json());

// In-Memory Database (Replace with MongoDB/PostgreSQL in production)
const usersDB = [];
const accountBalances = {};

// --- 2. JWT Authentication Middleware ---
const authenticateToken = (req, res, next) => {
    const authHeader = req.headers['authorization'];
    const token = authHeader && authHeader.split(' ')[1];

    if (!token) return res.status(401).json({ error: 'Access denied. Please log in.' });

    jwt.verify(token, process.env.JWT_SECRET, (err, user) => {
        if (err) return res.status(403).json({ error: 'Invalid or expired session.' });
        req.user = user;
        next();
    });
};

// --- 3. User Registration & Login Endpoints ---
app.post('/api/auth/register', async (req, res) => {
    try {
        const { email, password, accountType } = req.body; // accountType: 'demo' or 'real'
        
        const existingUser = usersDB.find(u => u.email === email);
        if (existingUser) return res.status(400).json({ error: 'Email already registered.' });

        const hashedPassword = await bcrypt.hash(password, 10);
        const newUser = {
            id: 'USER_' + Date.now(),
            email,
            password: hashedPassword,
            accountType: accountType || 'demo',
            cTraderAccountId: null,
            accessToken: null
        };

        usersDB.push(newUser);
        accountBalances[newUser.id] = accountType === 'demo' ? 10000.00 : 0.00;

        res.status(201).json({ message: 'Account created successfully. Please log in.' });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

app.post('/api/auth/login', async (req, res) => {
    const { email, password } = req.body;
    const user = usersDB.find(u => u.email === email);

    if (!user || !(await bcrypt.compare(password, user.password))) {
        return res.status(401).json({ error: 'Invalid email or password.' });
    }

    const token = jwt.sign(
        { id: user.id, email: user.email, accountType: user.accountType },
        process.env.JWT_SECRET,
        { expiresIn: '24h' }
    );

    res.json({
        message: 'Login successful',
        token,
        user: {
            id: user.id,
            email: user.email,
            accountType: user.accountType,
            balance: accountBalances[user.id]
        }
    });
});

// --- 4. cTrader Open API Connection Layer ---
class CTraderService {
    constructor(environment = 'demo') {
        this.host = environment === 'live' ? process.env.CTRADER_LIVE_HOST : process.env.CTRADER_DEMO_HOST;
        this.port = process.env.CTRADER_PORT;
        this.ws = null;
    }

    connect() {
        return new Promise((resolve, reject) => {
            const url = `wss://${this.host}:${this.port}`;
            this.ws = new WebSocket(url);

            this.ws.on('open', () => {
                this.authenticateApplication().then(resolve).catch(reject);
            });

            this.ws.on('error', (err) => reject(err));
        });
    }

    // Authorize application credentials (Client ID & Secret)
    authenticateApplication() {
        return new Promise((resolve) => {
            const appAuthReq = {
                clientPublicId: process.env.CTRADER_CLIENT_ID,
                clientSecret: process.env.CTRADER_CLIENT_SECRET
            };
            // Send Application Auth Request to cTrader OpenAPI Protobuf socket
            this.ws.send(JSON.stringify({ payloadType: 2100, payload: appAuthReq }));
            resolve({ status: 'Application Authenticated' });
        });
    }

    // Place Market/Limit/Stop Order
    sendOrder(ctraderAccountId, accessToken, orderDetails) {
        return new Promise((resolve, reject) => {
            if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
                return reject('cTrader connection offline.');
            }

            const protoOrderReq = {
                ctraderAccountId,
                symbolId: orderDetails.symbolId, // e.g., 1 for EURUSD
                tradeSide: orderDetails.action.toUpperCase(), // BUY or SELL
                orderType: orderDetails.orderType || "MARKET",
                volume: orderDetails.volume * 100000, // Converts lots to units
                stopLoss: orderDetails.stopLoss || null,
                takeProfit: orderDetails.takeProfit || null
            };

            // Transmit execution request to cTrader OpenAPI engine
            this.ws.send(JSON.stringify({ payloadType: 2106, payload: protoOrderReq }));
            
            resolve({
                status: 'SUCCESS',
                message: 'Order executed via cTrader API',
                details: protoOrderReq
            });
        });
    }
}

// Global cTrader instances
const ctraderDemo = new CTraderService('demo');
const ctraderLive = new CTraderService('live');

// Initialize Open API connections
ctraderDemo.connect().catch(console.error);
ctraderLive.connect().catch(console.error);

// --- 5. Protected Trading Endpoints ---

// Linking cTrader Access Token to User Session
app.post('/api/trading/connect-ctrader', authenticateToken, (req, res) => {
    const { ctraderAccountId, accessToken } = req.body;
    const user = usersDB.find(u => u.id === req.user.id);

    if (!user) return res.status(404).json({ error: 'User not found.' });

    user.cTraderAccountId = ctraderAccountId;
    user.accessToken = accessToken;

    res.json({ message: 'cTrader account successfully linked.' });
});

// Place Trade Endpoint
app.post('/api/trading/order', authenticateToken, async (req, res) => {
    try {
        const user = usersDB.find(u => u.id === req.user.id);
        const { symbolId, action, volume, stopLoss, takeProfit } = req.body;

        if (!symbolId || !action || !volume) {
            return res.status(400).json({ error: 'Missing required trade params (symbolId, action, volume).' });
        }

        const activeService = user.accountType === 'live' ? ctraderLive : ctraderDemo;
        
        const result = await activeService.sendOrder(user.cTraderAccountId, user.accessToken, {
            symbolId,
            action,
            volume,
            stopLoss,
            takeProfit
        });

        res.json(result);
    } catch (err) {
        res.status(500).json({ error: err.toString() });
    }
});

// --- 6. Wallet Endpoints (Deposits & Withdrawals) ---
app.post('/api/wallet/deposit', authenticateToken, (req, res) => {
    const { amount } = req.body;
    if (!amount || amount <= 0) return res.status(400).json({ error: 'Invalid deposit amount.' });

    accountBalances[req.user.id] = (accountBalances[req.user.id] || 0) + parseFloat(amount);

    res.json({
        message: 'Deposit successful.',
        newBalance: accountBalances[req.user.id]
    });
});

app.post('/api/wallet/withdraw', authenticateToken, (req, res) => {
    const { amount } = req.body;
    const currentBalance = accountBalances[req.user.id] || 0;

    if (!amount || amount <= 0) return res.status(400).json({ error: 'Invalid withdrawal amount.' });
    if (amount > currentBalance) return res.status(400).json({ error: 'Insufficient funds.' });

    accountBalances[req.user.id] -= parseFloat(amount);

    res.json({
        message: 'Withdrawal request processed successfully.',
        newBalance: accountBalances[req.user.id]
    });
});

// Account Summary Endpoint
app.get('/api/account/summary', authenticateToken, (req, res) => {
    const user = usersDB.find(u => u.id === req.user.id);
    res.json({
        userId: user.id,
        email: user.email,
        accountType: user.accountType,
        balance: accountBalances[user.id],
        cTraderConnected: !!user.cTraderAccountId
    });
});

// Start Express Server
const PORT = process.env.PORT || 5000;
app.listen(PORT, () => {
    console.log(`Lesego Markets Backend running on port ${PORT}`);
});