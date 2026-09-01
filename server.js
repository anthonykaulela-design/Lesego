require('dotenv').config();
const express = require('express');
const WebSocket = require('ws');
const cors = require('cors');
const bodyParser = require('body-parser');

const app = express();
app.use(cors());
app.use(bodyParser.json());

const PORT = process.env.PORT || 5000;
const CLIENT_ID = process.env.CTRADER_CLIENT_ID;
const CLIENT_SECRET = process.env.CTRADER_CLIENT_SECRET;

// cTrader Open API Host Endpoints
const HOSTS = {
    demo: 'wss://demo.ctraderapi.com:5035',
    live: 'wss://live.ctraderapi.com:5035'
};

// Active WebSocket connections pool per account
const activeConnections = {};

// Helper: Establish cTrader Open API WebSocket Connection
function getCTraderConnection(environment = 'demo') {
    const hostUrl = HOSTS[environment.toLowerCase()] || HOSTS.demo;
    return new Promise((resolve, reject) => {
        const ws = new WebSocket(hostUrl);

        ws.on('open', () => {
            console.log(`[Lesego Markets] Connected to cTrader ${environment.toUpperCase()} environment.`);
            
            // Step 1: Application Authentication Request
            const appAuthPayload = {
                clientMsgId: "APP_AUTH_" + Date.now(),
                payloadType: 2100, // ProtoOAApplicationAuthReq
                payload: {
                    clientId: CLIENT_ID,
                    clientSecret: CLIENT_SECRET
                }
            };
            ws.send(JSON.stringify(appAuthPayload));
            resolve(ws);
        });

        ws.on('error', (err) => {
            console.error(`[cTrader WS Error]:`, err);
            reject(err);
        });
    });
}

// ==========================================
// 1. ACCOUNT AUTHENTICATION & LOGIN ROUTE
// ==========================================
app.post('/api/account/login', async (req, res) => {
    const { cTraderAccountId, accessToken, environment } = req.body; 
    // environment: 'demo' or 'live'

    try {
        const ws = await getCTraderConnection(environment);
        
        // Step 2: Account Authorization Request
        const accountAuthPayload = {
            clientMsgId: "ACC_AUTH_" + Date.now(),
            payloadType: 2102, // ProtoOAAccountAuthReq
            payload: {
                ctidTraderAccountId: parseInt(cTraderAccountId),
                accessToken: accessToken
            }
        };

        ws.send(JSON.stringify(accountAuthPayload));
        
        // Store connection in memory pool
        activeConnections[cTraderAccountId] = { ws, environment };

        return res.status(200).json({
            status: 'success',
            message: `Account ${cTraderAccountId} authenticated on ${environment.toUpperCase()} server.`,
            accountId: cTraderAccountId,
            environment
        });
    } catch (error) {
        return res.status(500).json({ status: 'error', message: error.message });
    }
});

// ==========================================
// 2. ORDER EXECUTION & TRADING ROUTES
// ==========================================

// Place Trade (Market, Limit, Stop)
app.post('/api/trade/place', async (req, res) => {
    const { 
        cTraderAccountId, 
        symbolId, 
        tradeSide, // "BUY" or "SELL"
        volume,    // e.g., 100000 for 1 lot
        orderType, // "MARKET", "LIMIT", "STOP"
        stopLoss, 
        takeProfit,
        price      // Required for LIMIT/STOP
    } = req.body;

    const session = activeConnections[cTraderAccountId];
    if (!session) {
        return res.status(401).json({ status: 'error', message: 'Account not authenticated. Login first.' });
    }

    const newOrderPayload = {
        clientMsgId: "ORDER_" + Date.now(),
        payloadType: 2106, // ProtoOANewOrderReq
        payload: {
            ctidTraderAccountId: parseInt(cTraderAccountId),
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

    return res.status(200).json({
        status: 'success',
        message: 'Order order request sent to cTrader.',
        details: newOrderPayload.payload
    });
});

// Close Position
app.post('/api/trade/close', async (req, res) => {
    const { cTraderAccountId, positionId, volume } = req.body;
    const session = activeConnections[cTraderAccountId];

    if (!session) {
        return res.status(401).json({ status: 'error', message: 'Account session active not found.' });
    }

    const closePayload = {
        clientMsgId: "CLOSE_" + Date.now(),
        payloadType: 2110, // ProtoOAClosePositionReq
        payload: {
            ctidTraderAccountId: parseInt(cTraderAccountId),
            positionId: parseInt(positionId),
            volume: parseInt(volume)
        }
    };

    session.ws.send(JSON.stringify(closePayload));

    return res.status(200).json({ status: 'success', message: 'Position close request sent.' });
});

// Modify Position (SL / TP)
app.post('/api/trade/modify', async (req, res) => {
    const { cTraderAccountId, positionId, stopLoss, takeProfit } = req.body;
    const session = activeConnections[cTraderAccountId];

    if (!session) return res.status(401).json({ status: 'error', message: 'Session expired.' });

    const modifyPayload = {
        clientMsgId: "MODIFY_" + Date.now(),
        payloadType: 2111, // ProtoOAModifyPositionProtectionReq
        payload: {
            ctidTraderAccountId: parseInt(cTraderAccountId),
            positionId: parseInt(positionId),
            stopLoss: stopLoss ? parseFloat(stopLoss) : undefined,
            takeProfit: takeProfit ? parseFloat(takeProfit) : undefined
        }
    };

    session.ws.send(JSON.stringify(modifyPayload));
    return res.status(200).json({ status: 'success', message: 'Modification request transmitted.' });
});

// ==========================================
// 3. DEPOSIT & WITHDRAWAL (CRM / GATEWAY)
// ==========================================

// Deposit Endpoint (Processes funding into trading wallet)
app.post('/api/wallet/deposit', async (req, res) => {
    const { userId, cTraderAccountId, amount, paymentMethod, currency } = req.body;

    if (!amount || amount <= 0) {
        return res.status(400).json({ status: 'error', message: 'Invalid deposit amount.' });
    }

    // Integrated Payment Gateway Hook (Stripe / PayFast / Crypto)
    console.log(`[Lesego Markets CRM] Processing ${currency || 'USD'} ${amount} deposit for Account ${cTraderAccountId}`);

    // Mock CRM Wallet Database Update & cTrader Manager Balance Direct Call
    const transactionId = "DEP_" + Math.floor(100000 + Math.random() * 900000);

    return res.status(200).json({
        status: 'success',
        message: 'Deposit request processed successfully.',
        transactionId: transactionId,
        accountId: cTraderAccountId,
        creditedAmount: amount,
        currency: currency || 'USD'
    });
});

// Withdrawal Endpoint (Submits cash payout request)
app.post('/api/wallet/withdraw', async (req, res) => {
    const { userId, cTraderAccountId, amount, payoutDetails } = req.body;

    if (!amount || amount <= 0) {
        return res.status(400).json({ status: 'error', message: 'Invalid withdrawal amount.' });
    }

    const transactionId = "WTH_" + Math.floor(100000 + Math.random() * 900000);

    console.log(`[Lesego Markets CRM] Withdrawal request ${transactionId} received for ${cTraderAccountId}`);

    return res.status(200).json({
        status: 'success',
        message: 'Withdrawal request submitted for compliance verification.',
        transactionId: transactionId,
        requestedAmount: amount,
        status: 'Pending Approval'
    });
});

// Start Server
app.listen(PORT, () => {
    console.log(`====================================================`);
    console.log(`Lesego Markets Trading Backend Running on Port ${PORT}`);
    console.log(`cTrader Open API Client ID Configured: ${CLIENT_ID.substring(0, 10)}...`);
    console.log(`====================================================`);
});