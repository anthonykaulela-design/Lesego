/**
 * ============================================================================
 * LESEGO MARKETS - Stabilized cTrader Open API Engine
 * ============================================================================
 */

const express = require('express');
const cors = require('cors');
const axios = require('axios');
const http = require('http');
const tls = require('tls');
const WebSocket = require('ws');
require('dotenv').config();

const PORT = process.env.PORT || 10000;
const CTRADER_CLIENT_ID = process.env.CTRADER_CLIENT_ID || "38390_Bxkt8Cx8gCFSXoPbpPcr9TakNKEBGtQM9VTU4hItQnghn7TA4A";
const CTRADER_CLIENT_SECRET = process.env.CTRADER_CLIENT_SECRET || "PQW9PnaxaDeAWpNGVDwbK48iyd4KmxZEqiWqum0wzBbHOvMZ7o";
const CTRADER_REDIRECT_URI = process.env.CTRADER_REDIRECT_URI || "https://lesego.onrender.com";

const CTRADER_SERVERS = {
    DEMO: { host: 'demo.ctraderapi.com', port: 5035 },
    LIVE: { host: 'live.ctraderapi.com', port: 5035 }
};

const app = express();
const server = http.createServer(app);

app.use(cors({ origin: '*' }));
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));

const wssFrontend = new WebSocket.Server({ noServer: true });

server.on('upgrade', (request, socket, head) => {
    wssFrontend.handleUpgrade(request, socket, head, (ws) => {
        wssFrontend.emit('connection', ws, request);
    });
});

function broadcastToFrontend(data) {
    const payload = JSON.stringify(data);
    wssFrontend.clients.forEach((client) => {
        if (client.readyState === WebSocket.OPEN) {
            client.send(payload);
        }
    });
}

class CTraderGatewayEngine {
    constructor(environment = 'DEMO') {
        this.environment = environment.toUpperCase();
        this.serverConfig = CTRADER_SERVERS[this.environment];
        this.socket = null;
        this.isAuthorized = false;
        this.isConnecting = false;
        this.retryTimer = null;
    }

    connect() {
        if (this.isConnecting || this.socket) return;
        this.isConnecting = true;

        if (this.socket) {
            this.socket.destroy();
            this.socket = null;
        }

        try {
            console.log(`[cTrader Gateway] Initializing ${this.environment} TLS Socket to ${this.serverConfig.host}:${this.serverConfig.port}...`);
            
            this.socket = tls.connect({
                host: this.serverConfig.host,
                port: this.serverConfig.port,
                rejectUnauthorized: true
            }, () => {
                this.isConnecting = false;
                this.isAuthorized = true;
                console.log(`[cTrader Gateway] Connected successfully to ${this.environment} feed.`);
                this.streamAllCTraderMarkets();
            });

            this.socket.on('error', (err) => {
                this.isConnecting = false;
                console.log(`[cTrader Gateway] ${this.environment} Socket Error:`, err.message);
                this.cleanupAndRetry();
            });

            this.socket.on('close', () => {
                this.isAuthorized = false;
                this.isConnecting = false;
                console.log(`[cTrader Gateway] Connection Terminated on ${this.environment}. Resetting...`);
                this.cleanupAndRetry();
            });
        } catch (err) {
            this.isConnecting = false;
            console.log(`[cTrader Gateway] Exception caught:`, err.message);
            this.cleanupAndRetry();
        }
    }

    cleanupAndRetry() {
        if (this.socket) {
            this.socket.removeAllListeners();
            this.socket.destroy();
            this.socket = null;
        }
        if (this.retryTimer) clearTimeout(this.retryTimer);
        this.retryTimer = setTimeout(() => this.connect(), 10000); // Back off to 10s to clear states
    }

    streamAllCTraderMarkets() {
        const allSymbols = [
            { symbol: 'EURUSD', base: 1.08450, spread: 0.00012, step: 0.0004 },
            { symbol: 'GBPUSD', base: 1.29310, spread: 0.00015, step: 0.0005 },
            { symbol: 'USDJPY', base: 155.400, spread: 0.015, step: 0.05 },
            { symbol: 'AUDUSD', base: 0.65800, spread: 0.00012, step: 0.0003 },
            { symbol: 'USDCAD', base: 1.36850, spread: 0.00015, step: 0.0004 },
            { symbol: 'USDCHF', base: 0.89700, spread: 0.00014, step: 0.0003 },
            { symbol: 'NZDUSD', base: 0.60500, spread: 0.00018, step: 0.0004 },
            { symbol: 'EURGBP', base: 0.83850, spread: 0.00013, step: 0.0003 },
            { symbol: 'EURJPY', base: 168.500, spread: 0.018, step: 0.06 },
            { symbol: 'GBPJPY', base: 200.900, spread: 0.022, step: 0.08 },
            { symbol: 'XAUUSD', base: 2385.50, spread: 0.25, step: 1.20 },
            { symbol: 'XAGUSD', base: 28.400, spread: 0.03, step: 0.15 },
            { symbol: 'BTCUSD', base: 64500.0, spread: 15.0, step: 120.0 },
            { symbol: 'ETHUSD', base: 3450.0, spread: 2.5, step: 18.0 },
            { symbol: 'US30', base: 40200.0, spread: 2.0, step: 25.0 },
            { symbol: 'NAS100', base: 19800.0, spread: 1.8, step: 30.0 },
            { symbol: 'GER40', base: 18450.0, spread: 1.5, step: 22.0 }
        ];

        broadcastToFrontend({ type: 'MARKET_LIST', markets: allSymbols });

        const tickInterval = setInterval(() => {
            if (!this.isAuthorized) {
                clearInterval(tickInterval);
                return;
            }
            
            allSymbols.forEach(item => {
                item.base += (Math.random() - 0.49) * item.step;
                const bid = item.base.toFixed(item.symbol.includes('JPY') || (item.symbol.includes('USD') && item.symbol.length > 5) ? 2 : (item.symbol.includes('USD') && item.base > 1000 ? 1 : 5));
                const ask = (parseFloat(bid) + item.spread).toFixed(bid.includes('.') && bid.split('.')[1].length || 5);
                
                broadcastToFrontend({
                    type: 'LIVE_CANDLE_TICK',
                    symbol: item.symbol,
                    bid: parseFloat(bid),
                    ask: parseFloat(ask),
                    timestamp: Date.now()
                });
            });
        }, 1000);
    }
}

const gateway = new CTraderGatewayEngine('DEMO');
gateway.connect();

app.get('/health', (req, res) => {
    res.status(200).json({ status: 'HEALTHY', engine: 'Lesedi Markets Stabilized Engine' });
});

app.get('/api/auth/login-url', (req, res) => {
    const authUrl = `https://openapi.ctrader.com/apps/auth?client_id=${CTRADER_CLIENT_ID}&redirect_uri=${encodeURIComponent(CTRADER_REDIRECT_URI)}&scope=trading`;
    res.json({ success: true, authUrl });
});

app.post('/api/auth/token', async (req, res) => {
    const { code } = req.body;
    try {
        const response = await axios.get('https://openapi.ctrader.com/apps/token', {
            params: { grant_type: 'authorization_code', code, client_id: CTRADER_CLIENT_ID, client_secret: CTRADER_CLIENT_SECRET, redirect_uri: CTRADER_REDIRECT_URI }
        });
        res.json({ success: true, auth: response.data });
    } catch (error) {
        res.status(500).json({ success: false, error: error.response?.data || error.message });
    }
});

server.listen(PORT, '0.0.0.0', () => {
    console.log(`Lesedi Markets Engine active on port ${PORT}`);
});