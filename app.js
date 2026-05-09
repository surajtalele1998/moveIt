import { initializeApp } from "https://www.gstatic.com/firebasejs/10.7.1/firebase-app.js";
import { getFirestore, collection, doc, setDoc, onSnapshot, updateDoc, getDoc, query, where, orderBy, limit } from "https://www.gstatic.com/firebasejs/10.7.1/firebase-firestore.js";


/**
 * MoveIt SDK - Enhanced P2P Transfer Engine
 */
class MoveItSDK {
    constructor(config) {
        this.app = initializeApp(config.firebase);
        this.db = getFirestore(this.app);
        this.rtcConfig = {
            iceServers: [
                { urls: "stun:stun.l.google.com:19302" },
                { urls: "stun:stun1.l.google.com:19302" },
                { urls: "stun:stun.services.mozilla.com" }
            ],
            iceCandidatePoolSize: 10
        };
        this.peerConnection = null;
        this.dataChannel = null;
        this.onProgress = config.onProgress || (() => { });
        this.onStatus = config.onStatus || (() => { });
        this.onFileReceived = config.onFileReceived || (() => { });
        this.CHUNK_SIZE = 64 * 1024; // 64KB for better throughput in modern browsers
        this._initMessageInterface();
    }

    _initMessageInterface() {
        window.addEventListener('message', async (event) => {
            const { type, data } = event.data;
            if (type === 'MOVEIT_SEND_FILE') await this.createTransfer(data.file);
            if (type === 'MOVEIT_JOIN') await this.joinTransfer(data.pin);
        });
    }

    async createTransfer(files) {
        let fileToSend;
        if (files.length > 1) {
            this.onStatus("zipping");
            const zip = new JSZip();
            for (let i = 0; i < files.length; i++) {
                zip.file(files[i].name, files[i]);
            }
            fileToSend = await zip.generateAsync({ type: "blob" });
            fileToSend.name = "MoveIt_Archive.zip";
        } else {
            fileToSend = files[0];
        }

        this.currentFile = fileToSend;
        const pin = Math.floor(1000 + Math.random() * 9000).toString();

        this.peerConnection = new RTCPeerConnection(this.rtcConfig);
        this.dataChannel = this.peerConnection.createDataChannel("fileTransfer", { ordered: true });
        this._setupDataChannel(this.dataChannel);

        const callDoc = doc(collection(this.db, "calls"), pin);
        this.peerConnection.onicecandidate = (e) => e.candidate && setDoc(doc(collection(callDoc, "offerCandidates")), e.candidate.toJSON());

        const offer = await this.peerConnection.createOffer();
        await this.peerConnection.setLocalDescription(offer);

        await setDoc(callDoc, {
            offer: {
                sdp: offer.sdp,
                type: offer.type,
                fileName: fileToSend.name,
                fileSize: fileToSend.size,
                fileType: fileToSend.type
            },
            createdAt: Date.now()
        });

        onSnapshot(callDoc, (snap) => {
            const data = snap.data();
            if (data?.answer && !this.peerConnection.currentRemoteDescription) {
                this.peerConnection.setRemoteDescription(new RTCSessionDescription(data.answer));
            }
        });

        onSnapshot(collection(callDoc, "answerCandidates"), (snap) => {
            snap.docChanges().forEach(c => c.type === "added" && this.peerConnection.addIceCandidate(new RTCIceCandidate(c.doc.data())));
        });

        return pin;
    }

    async joinTransfer(pin) {
        const callDoc = doc(collection(this.db, "calls"), pin);
        const callData = (await getDoc(callDoc)).data();
        if (!callData) throw new Error("Invalid PIN");

        if (callData.type === 'api-transfer') {
            this.onStatus("fetching");
            const response = await fetch(`https://moveit-api-inox.onrender.com/receive/${pin}`);
            if (!response.ok) throw new Error("Transfer expired or invalid");

            const reader = response.body.getReader();
            const total = parseInt(response.headers.get('Content-Length'));
            let received = 0;
            const chunks = [];
            const startTime = Date.now();

            while (true) {
                const { done, value } = await reader.read();
                if (done) break;
                chunks.push(value);
                received += value.length;
                const elapsed = (Date.now() - startTime) / 1000;
                this.onProgress({
                    percent: (received / total) * 100,
                    speed: (received / (1024 * 1024 * elapsed)).toFixed(2),
                    receivedSize: received,
                    totalSize: total
                });
            }

            this.onFileReceived(new Blob(chunks, { type: callData.fileType }), callData.fileName);
            return;
        }

        this.currentFile = { name: callData.offer.fileName, size: callData.offer.fileSize, type: callData.offer.fileType };
        this.peerConnection = new RTCPeerConnection(this.rtcConfig);
        this.peerConnection.ondatachannel = (e) => {
            this.dataChannel = e.channel;
            this._setupDataChannel(this.dataChannel);
        };

        this.peerConnection.onicecandidate = (e) => e.candidate && setDoc(doc(collection(callDoc, "answerCandidates")), e.candidate.toJSON());

        await this.peerConnection.setRemoteDescription(new RTCSessionDescription(callData.offer));
        const answer = await this.peerConnection.createAnswer();
        await this.peerConnection.setLocalDescription(answer);
        await updateDoc(callDoc, { answer: { type: answer.type, sdp: answer.sdp } });

        onSnapshot(collection(callDoc, "offerCandidates"), (snap) => {
            snap.docChanges().forEach(c => c.type === "added" && this.peerConnection.addIceCandidate(new RTCIceCandidate(c.doc.data())));
        });
    }

    _setupDataChannel(channel) {
        let chunks = [];
        let size = 0;
        let start = 0;

        channel.onopen = () => {
            this.onStatus("connected");
            if (this.currentFile instanceof Blob) this._streamFile(channel);
        };

        channel.onmessage = (e) => {
            if (typeof e.data === 'string') {
                const msg = JSON.parse(e.data);
                if (msg.type === 'start') { chunks = []; size = 0; start = Date.now(); }
                if (msg.type === 'end') this.onFileReceived(new Blob(chunks, { type: this.currentFile.type }), this.currentFile.name);
            } else {
                chunks.push(e.data);
                size += e.data.byteLength;
                const elapsed = (Date.now() - start) / 1000;
                this.onProgress({
                    percent: (size / this.currentFile.size) * 100,
                    speed: (size / (1024 * 1024 * elapsed)).toFixed(2),
                    receivedSize: size,
                    totalSize: this.currentFile.size
                });
            }
        };
    }

    async _streamFile(channel) {
        channel.send(JSON.stringify({ type: 'start' }));
        const reader = this.currentFile.stream().getReader();
        let offset = 0;
        const start = Date.now();

        while (true) {
            const { done, value } = await reader.read();
            if (done) break;

            for (let i = 0; i < value.length; i += this.CHUNK_SIZE) {
                const chunk = value.slice(i, i + this.CHUNK_SIZE);
                while (channel.bufferedAmount > 4 * 1024 * 1024) await new Promise(r => setTimeout(r, 1));
                channel.send(chunk);
                offset += chunk.length;
                const elapsed = (Date.now() - start) / 1000;
                this.onProgress({
                    percent: (offset / this.currentFile.size) * 100,
                    speed: (offset / (1024 * 1024 * elapsed)).toFixed(2),
                    offset,
                    totalSize: this.currentFile.size
                });
            }
        }
        channel.send(JSON.stringify({ type: 'end' }));
    }

    disconnect() {
        if (this.peerConnection) this.peerConnection.close();
        this.onStatus("disconnected");
    }
}

// --- UI Application Logic ---
const firebaseConfig = {
    apiKey: "AIzaSyDPEao3L9V_hIpzFyD6C4hu7s7G8cgU6po",
    authDomain: "moveit-sync.firebaseapp.com",
    projectId: "moveit-sync",
    storageBucket: "moveit-sync.firebasestorage.app",
    messagingSenderId: "726005072326",
    appId: "1:726005072326:web:b852e74c9f1f6c551c72c7"
};

const sdk = new MoveItSDK({
    firebase: firebaseConfig,
    onStatus: (s) => {
        const el = document.getElementById('connectionStatus');
        const msgs = { connected: "Devices Connected! ⚡", zipping: "Archiving files... 📦", fetching: "Downloading from API... ☁️", disconnected: "P2P Multi-Device Sync" };
        if (el) el.innerText = msgs[s] || msgs.disconnected;
        if (['connected', 'fetching'].includes(s)) showView('view-transfer');
    },
    onProgress: (p) => {
        updateProgress(p.percent, `${p.speed} MB/s`, `${((p.receivedSize || p.offset) / (1024 * 1024)).toFixed(1)} / ${(p.totalSize / (1024 * 1024)).toFixed(1)} MB`);
    },
    onFileReceived: (blob, name) => {
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = name;
        a.click();
        showToast("File received successfully!");
        setTimeout(() => switchTab('receive'), 2000);
    }
});

window.MoveIt = sdk;

// UI Handlers
window.switchTab = (t) => {
    document.querySelectorAll('.tab, .view').forEach(el => el.classList.remove('active'));
    document.getElementById(`tab-${t}`).classList.add('active');
    document.getElementById(`view-${t}`).classList.add('active');
    if (t === 'send') resetSendView();
    if (t === 'dev') loadDevStats();
    if (t === 'history') loadHistory();
    if (t === 'resources') console.log("Resources loaded");

    // Proximity Update
    registerPresence(t);

};




function showView(id) {
    document.querySelectorAll('.view').forEach(v => v.classList.remove('active'));
    document.getElementById(id).classList.add('active');
}

function updateProgress(pct, speed, size) {
    document.getElementById('progressFill').style.width = `${pct}%`;
    document.getElementById('transferPercent').innerText = `${Math.round(pct)}%`;
    document.getElementById('transferSpeed').innerText = speed;
    document.getElementById('transferSize').innerText = size;
}

function showToast(m) {
    const t = document.getElementById('toast');
    t.innerText = m;
    t.style.transform = 'translateX(-50%) translateY(0)';
    t.style.opacity = '1';
    setTimeout(() => { t.style.transform = 'translateX(-50%) translateY(100px)'; t.style.opacity = '0'; }, 3000);
}

window.initSend = async () => {
    const files = document.getElementById('fileInput').files;
    if (!files.length) return showToast("Select files first");

    const btn = document.getElementById('btn-init-send');
    const originalText = btn.innerHTML;
    btn.innerHTML = "<span>⏳ Connecting to Cloud...</span>";
    btn.disabled = true;

    try {
        const pin = await sdk.createTransfer(files);
        registerPresence('send', pin); // Update presence with PIN
        document.getElementById('displayPIN').innerText = pin;

        document.getElementById('qrcode').innerHTML = "";
        new QRCode(document.getElementById("qrcode"), { text: `${window.location.origin}${window.location.pathname}?pin=${pin}`, width: 180, height: 180, colorDark: "#6366f1", colorLight: "#ffffff" });
        document.getElementById('send-ready').style.display = 'block';
        document.getElementById('btn-init-send').style.display = 'none';
        document.getElementById('btn-clear-files').style.display = 'none';

        // Save to History
        saveToHistory({
            pin,
            name: files.length > 1 ? `${files.length} Files` : files[0].name,
            size: Array.from(files).reduce((acc, f) => acc + f.size, 0),
            date: Date.now()
        });

        if (navigator.share) document.getElementById('shareBtn').style.display = 'block';
    } catch (e) {
        console.error(e);
        showToast("Error: " + (e.message || "Cloud connection failed"));
    } finally {
        btn.innerHTML = originalText;
        btn.disabled = false;
    }
};


window.connectToSender = async () => {
    const pin = document.getElementById('inputPIN').value;
    try {
        await sdk.joinTransfer(pin);
    } catch (e) {
        showToast(e.message);
    }
};

window.copyTransferLink = () => {
    const pin = document.getElementById('displayPIN').innerText;
    const url = `${window.location.origin}${window.location.pathname}?pin=${pin}`;
    copyToClipboard(url);
};

window.shareTransfer = async () => {
    const pin = document.getElementById('displayPIN').innerText;
    const url = `${window.location.origin}${window.location.pathname}?pin=${pin}`;
    const shareData = {
        title: 'MoveIt File Transfer',
        text: `I'm sending you files via MoveIt. Enter PIN: ${pin}`,
        url: url
    };

    if (navigator.share) {
        try {
            await navigator.share(shareData);
        } catch (e) {
            copyToClipboard(url);
        }
    } else {
        copyToClipboard(url);
    }
};

function copyToClipboard(text) {
    navigator.clipboard.writeText(text);
    showToast("Link copied to clipboard!");
}


window.startScanner = () => {
    const r = document.getElementById('reader');
    r.style.display = 'block';
    const s = new Html5QrcodeScanner("reader", { fps: 10, qrbox: 250 });
    s.render((txt) => {
        const pin = txt.includes('pin=') ? txt.split('pin=')[1] : txt;
        document.getElementById('inputPIN').value = pin;
        s.clear();
        r.style.display = 'none';
        window.connectToSender();
    });
};

// Developer Console Logic
window.generateApiKey = async () => {
    try {
        const res = await fetch('https://moveit-api-inox.onrender.com/api/v1/keys/generate', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ owner: 'Web User' })
        });
        const data = await res.json();
        if (data.apiKey) {
            localStorage.setItem('moveit_api_key', data.apiKey);
            showApiKey(data.apiKey);
            showToast("API Key Generated!");
        }
    } catch (e) {
        showToast("Error generating API key");
    }
};

function showApiKey(key) {
    document.getElementById('api-key-section').style.display = 'none';
    document.getElementById('key-display').style.display = 'block';
    document.getElementById('apiKeyText').innerText = key;
    fetchUsage(key);
}

async function fetchUsage(key) {
    try {
        const res = await fetch('https://moveit-api-inox.onrender.com/api/v1/usage', {
            headers: { 'X-API-Key': key }
        });
        const data = await res.json();
        if (data.totalTransfers !== undefined) {
            document.getElementById('stat-tier').innerText = data.tier || 'Free';
            document.getElementById('stat-data').innerText = `${(data.totalBytes / (1024 * 1024)).toFixed(1)} MB`;

            const pct = (data.totalTransfers / data.quota) * 100;
            document.getElementById('quotaFill').style.width = `${pct}%`;
            document.getElementById('stat-quota').innerText = `${data.totalTransfers} / ${data.quota} transfers`;

            listenToLogs(key);
        }
    } catch (e) { console.error("Stats fetch failed"); }
}

function listenToLogs(key) {
    const logsCol = collection(sdk.db, "apiLogs");
    const q = query(logsCol, where("apiKey", "==", key), orderBy("timestamp", "desc"), limit(5));

    onSnapshot(q, (snap) => {
        const logList = document.getElementById('log-list');
        if (snap.empty) return;

        logList.innerHTML = snap.docs.map(doc => {
            const log = doc.data();
            const time = new Date(log.timestamp).toLocaleTimeString();
            return `<p class="log-entry">[${time}] ${log.event} -> ${log.status}</p>`;
        }).join('');
    });
}


function loadDevStats() {
    const key = localStorage.getItem('moveit_api_key');
    if (key) showApiKey(key);
}

window.copyApiKey = () => {
    const key = document.getElementById('apiKeyText').innerText;
    navigator.clipboard.writeText(key);
    showToast("API Key copied!");
};

// Proximity & Discovery Logic
window.registerPresence = async (mode, pin = null) => {
    try {
        const deviceType = /Mobile|Android|iPhone/i.test(navigator.userAgent) ? 'Mobile' : 'Desktop';
        await fetch('https://moveit-api-inox.onrender.com/api/v1/presence', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ deviceType, pin, mode })
        });
    } catch (e) { }
};

window.checkNearby = async () => {
    try {
        const res = await fetch('https://moveit-api-inox.onrender.com/api/v1/nearby');
        const nearby = await res.json();
        const tabEl = document.querySelector('.tab.active');
        if (!tabEl) return;

        const currentMode = tabEl.id.replace('tab-', '');
        const listId = currentMode === 'send' ? 'nearby-receivers' : 'nearby-senders';
        const containerId = currentMode === 'send' ? 'nearby-send' : 'nearby-receive';
        const targetMode = currentMode === 'send' ? 'receive' : 'send';

        const matches = nearby.filter(n => n.mode === targetMode && n.pin);

        const listEl = document.getElementById(listId);
        const containerEl = document.getElementById(containerId);
        if (!listEl || !containerEl) return;

        if (matches.length > 0) {
            containerEl.style.display = 'block';
            document.getElementById('nearby-status').style.display = 'block';
            listEl.innerHTML = matches.map(m => `
                <div class="nearby-device" onclick="connectToNearby('${m.pin}')">
                    <div style="display: flex; align-items: center;">
                        <span class="device-icon">${m.deviceType === 'Mobile' ? '📱' : '💻'}</span>
                        <span class="device-type">${m.deviceType} Device</span>
                    </div>
                    <span class="history-pin">${m.pin}</span>
                </div>
            `).join('');
        } else {
            containerEl.style.display = 'none';
        }
    } catch (e) { }
};

window.connectToNearby = (pin) => {
    const isReceive = document.getElementById('tab-receive').classList.contains('active');
    if (isReceive) {
        document.getElementById('inputPIN').value = pin;
        window.connectToSender();
    } else {
        showToast(`Receiver is ready! PIN: ${pin}`);
    }
};

setInterval(window.checkNearby, 5000);
window.addEventListener('load', () => window.registerPresence('send'));


// History Management
function saveToHistory(item) {
    const history = JSON.parse(localStorage.getItem('moveit_history') || '[]');
    history.unshift(item);
    localStorage.setItem('moveit_history', JSON.stringify(history.slice(0, 10))); // Keep last 10
}

window.loadHistory = () => {
    const list = document.getElementById('history-list');
    const history = JSON.parse(localStorage.getItem('moveit_history') || '[]');

    if (history.length === 0) {
        list.innerHTML = '<p class="subtitle" style="text-align: center;">No transfers yet.</p>';
        return;
    }

    list.innerHTML = history.map(item => `
        <div class="history-item">
            <div class="history-info">
                <span class="history-name">${item.name}</span>
                <span class="history-meta">${(item.size / (1024 * 1024)).toFixed(2)} MB • ${new Date(item.date).toLocaleTimeString()}</span>
            </div>
            <div class="history-pin" onclick="navigator.clipboard.writeText('${item.pin}'); showToast('PIN Copied!')">${item.pin}</div>
        </div>
    `).join('');
};

window.clearHistory = () => {
    localStorage.removeItem('moveit_history');
    loadHistory();
    showToast("History cleared");
};

window.clearFiles = () => {
    resetSendView();
    showToast("Selection cleared");
};

function resetSendView() {
    document.getElementById('send-ready').style.display = 'none';
    document.getElementById('btn-init-send').style.display = 'flex';
    document.getElementById('btn-clear-files').style.display = 'none';
    document.getElementById('fileInput').value = "";
    document.getElementById('fileNameDisplay').innerText = "Drop files here or click to browse";
    sdk.disconnect();
}


// Auto-join if PIN in URL
window.addEventListener('load', () => {
    const params = new URLSearchParams(window.location.search);
    const pin = params.get('pin');
    if (pin) {
        switchTab('receive');
        document.getElementById('inputPIN').value = pin;
        window.connectToSender();
    }
});

document.getElementById('fileInput').addEventListener('change', (e) => {
    const count = e.target.files.length;
    document.getElementById('fileNameDisplay').innerText = count > 1 ? `${count} files selected` : (e.target.files[0]?.name || "No file selected");
    document.getElementById('btn-clear-files').style.display = count > 0 ? 'flex' : 'none';
});

