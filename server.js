/**
 * ============================================================================
 * LESEGIS MARKETS - cTrader Open API Production Engine
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
// Updated credentials provided from your latest dashboard panel
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

class ResilientCTraderGateway {
    constructor(environment = 'DEMO') {
        this.environment = environment.toUpperCase();
        this.serverConfig = CTRADER_SERVERS[this.environment];
        this.socket = null;
        this.isAuthorized = false;
        this.isConnecting = false;
    }

    connect() {
        if (this.isConnecting) return;
        this.isConnecting = true;

        try {
            this.socket = tls.connect(this.serverConfig.port, this.serverConfig.host, { rejectUnauthorized: true }, () => {
                this.isConnecting = false;
                this.isAuthorized = true;
            });

            this.socket.on('error', () => { this.isConnecting = false; });
            this.socket.on('close', () => {
                this.isAuthorized = false;
                this.isConnecting = false;
                setTimeout(() => this.connect(), 5000);
            });
        } catch (err) {
            this.isConnecting = false;
            setTimeout(() => this.connect(), 5000);
        }
    }
}

const gateways = {
    DEMO: new ResilientCTraderGateway('DEMO'),
    LIVE: new ResilientCTraderGateway('LIVE')
};

gateways.DEMO.connect();
gateways.LIVE.connect();

app.get('/health', (req, res) => {
    res.status(200).json({ status: 'HEALTHY', engine: 'Lesego Markets Gateway', timestamp: new Date().toISOString() });
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
                code,
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

server.listen(PORT, '0.0.0.0', () => {
    console.log(`Engine active on port ${PORT}`);
});