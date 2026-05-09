const express = require('express');
const multer = require('multer');
const { initializeApp, cert } = require('firebase-admin/app');
const { getFirestore } = require('firebase-admin/firestore');
const { getStorage } = require('firebase-admin/storage');
const cors = require('cors');
const QRCode = require('qrcode');
const axios = require('axios'); // For webhooks

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

// Busboy or similar would be better for 3GB, but we'll optimize Multer to use disk
const upload = multer({ 
    storage: multer.diskStorage({}), // Use temp disk instead of RAM
    limits: { fileSize: 5 * 1024 * 1024 * 1024 } // 5GB Limit
});
const JSZip = require('jszip');
const fs = require('fs');

/**
 * POST /transfer
 * Initiates a transfer with multiple files.
 */
app.post('/transfer', upload.array('files', 10), async (req, res) => {
  try {
    const files = req.files;
    const { filename, webhookUrl, ttl } = req.query;

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

    finalStream.pipe(blobStream);

    // In-house QR Generation
    const joinUrl = `${process.env.FRONTEND_URL || 'https://moveit.onrender.com'}?pin=${pin}`;
    const qrBase64 = await QRCode.toDataURL(joinUrl);

    blobStream.on('error', (err) => { throw err; });
    blobStream.on('finish', async () => {
        // 2. Create Signaling Doc
        await db.collection('calls').doc(pin).set({
            type: 'api-transfer',
            fileName: finalName,
            fileSize: finalSize,
            fileType: finalType,
            storagePath: blob.name,
            webhookUrl: webhookUrl || null,
            createdAt: Date.now(),
            expiresAt: Date.now() + (parseInt(ttl) || 3600) * 1000
        });

        res.json({
            pin,
            qr: qrBase64,
            joinUrl: joinUrl,
            status: "ready"
        });
    });

    blobStream.end(finalBuffer);

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

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`MoveIt Premium API running` ));
