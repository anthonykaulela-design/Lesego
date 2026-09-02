const express = require('express');
const cors = require('cors');
const jwt = require('jsonwebtoken');
const bcrypt = require('bcryptjs');
const axios = require('axios');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 5000;
const JWT_SECRET = process.env.JWT_SECRET || 'lesego_markets_fallback_secret';

// --- CORS Configuration (Prevents Browser Blocking) ---
app.use(cors({
    origin: '*',
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization']
}));

app.use(express.json());

// --- In-Memory Datastore (Connect MongoDB/PostgreSQL in production) ---
const users = [];
const userBalances = {}; // { userId: { DEMO: 10000, REAL: 0 } }
const userPositions = []; // Active trades

// cTrader API Endpoint Configurations
const CTRADER_ENDPOINTS = {
    DEMO: 'https://demo.ctraderapi.com',
    REAL: 'https://live.ctraderapi.com'
};

// --- Authentication Middleware ---
const authenticateToken = (req, res, next) => {
    const authHeader = req.headers['authorization'];
    const token = authHeader && authHeader.split(' ')[1];

    if (!token) {
        return res.status(401).json({ success: false, message: 'Access denied. Please login first.' });
    }

    jwt.verify(token, JWT_SECRET, (err, user) => {
        if (err) {
            return res.status(403).json({ success: false, message: 'Session expired or invalid token.' });
        }
        req.user = user;
        next();
    });
};

// --- Health Check for Render Deployment ---
app.get('/', (req, res) => {
    res.status(200).json({ status: 'Online', service: 'Lesego Markets API Engine', timestamp: new Date() });
});

// ==========================================
// 1. AUTHENTICATION ROUTES (Register & Login)
// ==========================================

// Register User
app.post('/api/auth/register', async (req, res) => {
    try {
        const { fullName, email, password } = req.body;

        if (!email || !password || !fullName) {
            return res.status(400).json({ success: false, message: 'All fields are required.' });
        }

        const existingUser = users.find(u => u.email === email);
        if (existingUser) {
            return res.status(400).json({ success: false, message: 'User already exists.' });
        }

        const hashedPassword = await bcrypt.hash(password, 10);
        const userId = 'usr_' + Date.now();

        const newUser = { id: userId, fullName, email, password: hashedPassword, createdAt: new Date() };
        users.push(newUser);

        // Initialize Demo and Real Balances
        userBalances[userId] = { DEMO: 10000.00, REAL: 0.00 };

        res.status(201).json({ success: true, message: 'Registration successful. You can now login.' });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

// Login User
app.post('/api/auth/login', async (req, res) => {
    try {
        const { email, password } = req.body;

        const user = users.find(u => u.email === email);
        if (!user) {
            return res.status(400).json({ success: false, message: 'Invalid email or password.' });
        }

        const validPassword = await bcrypt.compare(password, user.password);
        if (!validPassword) {
            return res.status(400).json({ success: false, message: 'Invalid email or password.' });
        }

        const token = jwt.sign({ userId: user.id, email: user.email }, JWT_SECRET, { expiresIn: '24h' });

        res.json({
            success: true,
            token,
            user: { id: user.id, fullName: user.fullName, email: user.email },
            balances: userBalances[user.id]
        });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

// Get User Profile & Balances
app.get('/api/user/profile', authenticateToken, (req, res) => {
    const user = users.find(u => u.id === req.user.userId);
    if (!user) return res.status(404).json({ success: false, message: 'User not found.' });

    res.json({
        success: true,
        user: { id: user.id, fullName: user.fullName, email: user.email },
        balances: userBalances[user.id] || { DEMO: 10000, REAL: 0 }
    });
});

// ==========================================
// 2. WALLET (Deposits & Withdrawals)
// ==========================================

// Deposit Funds
app.post('/api/wallet/deposit', authenticateToken, (req, res) => {
    const { amount, accountType } = req.body; // DEMO or REAL
    const type = (accountType || 'DEMO').toUpperCase();

    if (!amount || amount <= 0) {
        return res.status(400).json({ success: false, message: 'Invalid deposit amount.' });
    }

    userBalances[req.user.userId][type] += parseFloat(amount);

    res.json({
        success: true,
        message: `Successfully deposited $${amount} to ${type} account.`,
        newBalance: userBalances[req.user.userId][type]
    });
});

// Withdraw Funds
app.post('/api/wallet/withdraw', authenticateToken, (req, res) => {
    const { amount, accountType } = req.body;
    const type = (accountType || 'REAL').toUpperCase();

    if (!amount || amount <= 0) {
        return res.status(400).json({ success: false, message: 'Invalid withdrawal amount.' });
    }

    if (userBalances[req.user.userId][type] < amount) {
        return res.status(400).json({ success: false, message: 'Insufficient funds.' });
    }

    userBalances[req.user.userId][type] -= parseFloat(amount);

    res.json({
        success: true,
        message: `Successfully withdrew $${amount} from ${type} account.`,
        newBalance: userBalances[req.user.userId][type]
    });
});

// ==========================================
// 3. TRADING & cTRADER API INTEGRATION
// ==========================================

// cTrader OAuth Credentials Integration Endpoint
app.get('/api/ctrader/credentials', authenticateToken, (req, res) => {
    res.json({
        clientId: process.env.CTRADER_CLIENT_ID,
        redirectUri: 'https://lesegomarkets.com/oauth/callback'
    });
});

// Place Trade Order (Supports Demo & Real Accounts)
app.post('/api/trade/order', authenticateToken, async (req, res) => {
    try {
        const { symbol, tradeType, volume, accountType, stopLoss, takeProfit } = req.body;
        const mode = (accountType || 'DEMO').toUpperCase(); // DEMO or REAL

        if (!symbol || !tradeType || !volume) {
            return res.status(400).json({ success: false, message: 'Symbol, trade type, and volume are required.' });
        }

        const position = {
            tradeId: 'trd_' + Date.now(),
            userId: req.user.userId,
            accountType: mode,
            symbol,
            tradeType: tradeType.toUpperCase(), // BUY or SELL
            volume,
            openPrice: symbol.includes('XAU') ? 2500.50 : 1.0850, // Mock price engine
            stopLoss: stopLoss || null,
            takeProfit: takeProfit || null,
            openedAt: new Date()
        };

        userPositions.push(position);

        res.status(201).json({
            success: true,
            message: `Trade executed on ${mode} account via Lesego Markets Engine.`,
            trade: position
        });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

// Get Active Trades
app.get('/api/trade/positions', authenticateToken, (req, res) => {
    const { accountType } = req.query;
    const mode = (accountType || 'DEMO').toUpperCase();

    const activeTrades = userPositions.filter(
        p => p.userId === req.user.userId && p.accountType === mode
    );

    res.json({ success: true, accountType: mode, positions: activeTrades });
});

// Close Trade
app.post('/api/trade/close', authenticateToken, (req, res) => {
    const { tradeId } = req.body;
    const index = userPositions.findIndex(p => p.tradeId === tradeId && p.userId === req.user.userId);

    if (index === -1) {
        return res.status(404).json({ success: false, message: 'Position not found.' });
    }

    const closedTrade = userPositions.splice(index, 1)[0];
    res.json({ success: true, message: 'Trade closed successfully.', closedTrade });
});

// --- Server Listener for Render Deployment ---
app.listen(PORT, '0.0.0.0', () => {
    console.log(`Lesego Markets API Engine running on port ${PORT}`);
});