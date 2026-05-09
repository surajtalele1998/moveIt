const express = require('express');
const multer = require('multer');
const { initializeApp, cert } = require('firebase-admin/app');
const { getFirestore, FieldValue } = require('firebase-admin/firestore');

const { getStorage } = require('firebase-admin/storage');
const cors = require('cors');
const QRCode = require('qrcode');
const axios = require('axios'); // For webhooks
const rateLimit = require('express-rate-limit');


// --- Firebase Admin Setup ---
const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);

initializeApp({
    credential: cert(serviceAccount),
    storageBucket: "moveit-sync.firebasestorage.app"
});

const db = getFirestore();
const bucket = getStorage().bucket();
const app = express();

app.use(cors());
app.use(express.json());

// --- Rate Limiting ---
const apiLimiter = rateLimit({
    windowMs: 15 * 60 * 1000, // 15 minutes
    max: 100, // Limit each IP to 100 requests per windowMs
    message: { error: "TOO_MANY_REQUESTS", message: "Too many requests from this IP, please try again after 15 minutes" }
});

// --- API Key Middleware ---
const validateApiKey = async (req, res, next) => {
    const apiKey = req.header('X-API-Key');
    if (!apiKey) {
        // For backward compatibility or public use, we might allow it, 
        // but for "Premium" we should require it.
        // Let's assume third-party users MUST provide it.
        return res.status(401).json({ error: "MISSING_API_KEY", message: "API Key is required in X-API-Key header" });
    }

    const keyDoc = await db.collection('apiKeys').doc(apiKey).get();
    if (!keyDoc.exists) {
        return res.status(403).json({ error: "INVALID_API_KEY", message: "The provided API Key is invalid or expired" });
    }

    req.apiKeyData = keyDoc.data();
    req.apiKey = apiKey;
    next();
};

// --- Live Logging Helper ---
const logApiActivity = async (apiKey, event, status, details = {}) => {
    try {
        await db.collection('apiLogs').add({
            apiKey,
            event,
            status,
            details,
            timestamp: Date.now(),
            ip: details.ip || 'unknown'
        });
    } catch (e) { console.error("Logging failed", e.message); }
};



// Busboy or similar would be better for 3GB, but we'll optimize Multer to use disk
const upload = multer({
    storage: multer.diskStorage({}), // Use temp disk instead of RAM
    limits: { fileSize: 5 * 1024 * 1024 * 1024 } // 5GB Limit
});
const JSZip = require('jszip');
const fs = require('fs');

/**
 * POST /transfer
 * Initiates a transfer with multiple files. Requires API Key.
 */
app.post('/transfer', apiLimiter, validateApiKey, upload.array('files', 10), async (req, res) => {
    logApiActivity(req.apiKey, 'TRANSFER_INIT', 'success', { fileCount: req.files.length });


    try {
        const files = req.files;
        const { filename, webhookUrl, ttl, downloadLimit, metadata } = req.query;
        let finalStream, finalName, finalType, finalSize;



        if (!files || files.length === 0) return res.status(400).json({ error: "No files uploaded" });

        if (files.length > 1) {
            // For multiple massive files, we recommend P2P, but we can zip small ones
            const zip = new JSZip();
            for (const f of files) {
                zip.file(f.originalname, fs.createReadStream(f.path));
            }
            finalStream = zip.generateNodeStream({ type: 'nodebuffer', streamFiles: true });
            finalSize = null; // Chunked encoding
        } else {
            finalStream = fs.createReadStream(files[0].path);
            finalName = filename || files[0].originalname;
            finalType = files[0].mimetype;
            finalSize = files[0].size;
        }

        const pin = Math.floor(1000 + Math.random() * 9000).toString();
        const blob = bucket.file(`api-transfers/${pin}/${finalName}`);
        const blobStream = blob.createWriteStream({ metadata: { contentType: finalType } });

        // Trigger Webhook: transfer.started
        if (webhookUrl) {
            try {
                axios.post(webhookUrl, {
                    event: 'transfer.started',
                    pin: pin,
                    fileName: finalName,
                    timestamp: Date.now()
                }).catch(e => console.error("Webhook started failed", e.message));
            } catch (e) { }
        }

        finalStream.pipe(blobStream);


        // In-house QR Generation
        const joinUrl = `${process.env.FRONTEND_URL || 'https://moveit-app.onrender.com'}?pin=${pin}`;
        const qrBase64 = await QRCode.toDataURL(joinUrl);

        blobStream.on('error', (err) => { throw err; });
        blobStream.on('finish', async () => {
            // 2. Create Signaling Doc
            const transferData = {
                type: 'api-transfer',
                fileName: finalName,
                fileSize: finalSize,
                fileType: finalType,
                storagePath: blob.name,
                webhookUrl: webhookUrl || null,
                apiKey: req.apiKey, // Associate with API key
                createdAt: Date.now(),
                expiresAt: Date.now() + (parseInt(ttl) || 3600) * 1000,
                downloadLimit: parseInt(downloadLimit) || null,
                downloadCount: 0,
                metadata: metadata ? JSON.parse(metadata) : {}
            };


            await db.collection('calls').doc(pin).set(transferData);

            // Update Usage Analytics
            await db.collection('apiKeys').doc(req.apiKey).update({
                totalTransfers: FieldValue.increment(1),
                totalBytes: FieldValue.increment(finalSize || 0),
                lastUsed: Date.now()
            });


            res.json({
                pin,
                qr: qrBase64,
                joinUrl: joinUrl,
                status: "ready",
                expiresAt: transferData.expiresAt
            });
        });



    } catch (error) {

        console.error(error);
        res.status(500).json({ error: "Transfer failed to initialize" });
    }
});

/**
 * GET /receive/:pin
 * Streams the file back to the receiver.
 */
app.get('/receive/:pin', async (req, res) => {
    try {
        const pin = req.params.pin;
        const doc = await db.collection('calls').doc(pin).get();

        if (!doc.exists) return res.status(404).json({ error: "Invalid or expired PIN" });

        const data = doc.data();

        // Security check: Check expiration
        if (Date.now() > data.expiresAt) {
            await db.collection('calls').doc(pin).delete();
            return res.status(410).json({ error: "Transfer expired" });
        }

        // Security check: Check download limit
        if (data.downloadLimit && data.downloadCount >= data.downloadLimit) {
            await db.collection('calls').doc(pin).delete();
            return res.status(410).json({ error: "DOWNLOAD_LIMIT_REACHED", message: "This transfer has reached its maximum download limit" });
        }

        // Increment download count
        await db.collection('calls').doc(pin).update({
            downloadCount: FieldValue.increment(1)
        });

        const file = bucket.file(data.storagePath);


        res.setHeader('Content-Length', data.fileSize);
        res.setHeader('Content-Disposition', `attachment; filename="${data.fileName}"`);
        res.setHeader('Content-Type', data.fileType);

        const readStream = file.createReadStream();

        readStream.on('end', async () => {
            // Trigger Webhook if present
            if (data.webhookUrl) {
                try {
                    await axios.post(data.webhookUrl, {
                        event: 'transfer.completed',
                        pin: pin,
                        fileName: data.fileName,
                        timestamp: Date.now()
                    });
                } catch (e) { console.error("Webhook failed", e.message); }
            }
            // Cleanup optionally (uncomment if you want one-time use)
            // await db.collection('calls').doc(pin).delete();
            // await file.delete();
        });

        readStream.pipe(res);
    } catch (error) {
        res.status(500).json({ error: "Failed to retrieve file" });
    }
});

/**
 * GET /status/:pin
 * Returns current status of a transfer.
 */
app.get('/status/:pin', async (req, res) => {
    const doc = await db.collection('calls').doc(req.params.pin).get();
    if (!doc.exists) return res.status(404).json({ error: "Not found" });
    res.json(doc.data());
});

/**
 * GET /api/v1/usage
 * Returns usage statistics for the provided API key.
 */
app.get('/api/v1/usage', validateApiKey, async (req, res) => {
    logApiActivity(req.apiKey, 'USAGE_CHECK', 'success');
    res.json(req.apiKeyData);
});


/**
 * GET /api/v1/transfers
 * Lists active transfers created by this API key.
 */
app.get('/api/v1/transfers', validateApiKey, async (req, res) => {
    const snapshot = await db.collection('calls').where('apiKey', '==', req.apiKey).get();
    const transfers = [];
    snapshot.forEach(doc => transfers.push({ pin: doc.id, ...doc.data() }));
    res.json(transfers);
});

/**
 * DELETE /api/v1/transfer/:pin
 * Manually deletes a transfer.
 */
app.delete('/api/v1/transfer/:pin', validateApiKey, async (req, res) => {
    const pin = req.params.pin;
    const doc = await db.collection('calls').doc(pin).get();

    if (!doc.exists) return res.status(404).json({ error: "Transfer not found" });
    if (doc.data().apiKey !== req.apiKey) return res.status(403).json({ error: "UNAUTHORIZED", message: "You don't own this transfer" });

    // Cleanup Storage
    try {
        await bucket.file(doc.data().storagePath).delete();
    } catch (e) { console.error("Storage delete failed", e.message); }

    await db.collection('calls').doc(pin).delete();
    res.json({ status: "deleted", pin });
});

/**
 * PATCH /api/v1/transfer/:pin
 * Updates transfer settings (TTL or Webhook URL).
 */
app.patch('/api/v1/transfer/:pin', validateApiKey, async (req, res) => {
    const pin = req.params.pin;
    const { ttl, webhookUrl } = req.body;

    const docRef = db.collection('calls').doc(pin);
    const doc = await docRef.get();

    if (!doc.exists) return res.status(404).json({ error: "TRANSFER_NOT_FOUND", message: "Transfer not found" });
    if (doc.data().apiKey !== req.apiKey) return res.status(403).json({ error: "UNAUTHORIZED", message: "You don't own this transfer" });

    const updates = {};
    if (ttl) updates.expiresAt = Date.now() + (parseInt(ttl) || 3600) * 1000;
    if (webhookUrl) updates.webhookUrl = webhookUrl;

    await docRef.update(updates);
    res.json({ status: "updated", pin, updates });
});

/**
 * Robots.txt & Sitemap for Crawlers
 */
app.get('/robots.txt', (req, res) => {
    res.type('text/plain');
    res.send("User-agent: *\nAllow: /\nSitemap: https://moveit-app.onrender.com/sitemap.xml");
});

app.get('/sitemap.xml', (req, res) => {
    res.type('application/xml');
    res.send(`<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>https://moveit-app.onrender.com/</loc><priority>1.0</priority></url>
  <url><loc>https://moveit-app.onrender.com/api/docs</loc><priority>0.8</priority></url>
</urlset>`);
});

/**
 * GET /api/v1/spec
 * Machine-readable API specification for AI agents.
 */
app.get('/api/v1/spec', (req, res) => {
    res.json({
        openapi: "3.0.0",
        info: {
            title: "MoveIt API",
            description: "Professional File Transfer as a Service",
            version: "2.0.0"
        },
        endpoints: {
            "/transfer": "POST - Upload files (requires X-API-Key)",
            "/api/v1/usage": "GET - Check bandwidth and quotas",
            "/api/v1/nearby": "GET - Local network discovery",
            "/api/v1/presence": "POST - Register presence for discovery"
        }
    });
});

app.get('/api/docs', (req, res) => {
    res.send(`
        <!DOCTYPE html>
        <html>
        <head>
            <title>MoveIt API Docs</title>
            <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/water.css@2/out/water.css">
            <style>body { max-width: 800px; margin: 40px auto; padding: 20px; line-height: 1.6; }</style>
        </head>
        <body>
            <h1>MoveIt Premium API Documentation</h1>
            <p>Welcome to the professional file transfer API.</p>
            
            <h2>Authentication</h2>
            <p>Include your key in the <code>X-API-Key</code> header.</p>
            
            <h2>Endpoints</h2>
            <ul>
                <li><strong>POST /transfer</strong>: Create a new transfer.</li>
                <li><strong>GET /api/v1/transfers</strong>: List your active transfers.</li>
                <li><strong>GET /api/v1/usage</strong>: Check your data usage.</li>
                <li><strong>PATCH /api/v1/transfer/:pin</strong>: Update TTL or Webhook.</li>
                <li><strong>DELETE /api/v1/transfer/:pin</strong>: Manually expire a transfer.</li>
            </ul>
            
            <p>For more details, visit the <a href="/">MoveIt Dashboard</a>.</p>
        </body>
        </html>
    `);
});


/**
 * POST /api/v1/presence
 * Registers a device as 'online' on the current public IP.
 * Scoped to API Key if provided.
 */
app.post('/api/v1/presence', async (req, res) => {
    const ip = req.headers['x-forwarded-for'] || req.socket.remoteAddress;
    const apiKey = req.headers['x-api-key'] || 'public';
    const { deviceType, pin, mode } = req.body;

    const presenceId = `${apiKey}_${ip}_${pin || 'waiting'}`;
    await db.collection('presence').doc(presenceId).set({
        apiKey,
        ip,
        pin: pin || null,
        deviceType: deviceType || 'unknown',
        mode: mode || 'idle',
        lastSeen: Date.now()
    });
    res.json({ status: "online", scope: apiKey });
});

/**
 * GET /api/v1/nearby
 * Returns other devices on the same public IP and same API Key scope.
 */
app.get('/api/v1/nearby', async (req, res) => {
    const ip = req.headers['x-forwarded-for'] || req.socket.remoteAddress;
    const apiKey = req.headers['x-api-key'] || 'public';

    const snapshot = await db.collection('presence')
        .where('ip', '==', ip)
        .where('apiKey', '==', apiKey)
        .where('lastSeen', '>', Date.now() - 30000)
        .get();

    const nearby = [];
    snapshot.forEach(doc => {
        const data = doc.data();
        nearby.push(data);
    });
    res.json(nearby);
});


app.post('/api/v1/keys/generate', async (req, res) => {
    // In a real app, this would be behind a user session/admin auth
    const newKey = 'mv_' + Math.random().toString(36).substring(2, 15) + Math.random().toString(36).substring(2, 15);
    await db.collection('apiKeys').doc(newKey).set({
        totalTransfers: 0,
        totalBytes: 0,
        createdAt: Date.now(),
        lastUsed: null,
        tier: req.body.tier || 'Free',
        quota: req.body.tier === 'Pro' ? 1000 : 100, // Monthly transfer limit
        owner: req.body.owner || 'anonymous'
    });
    res.json({ apiKey: newKey, tier: req.body.tier || 'Free' });
});



const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`MoveIt Premium API running`));
