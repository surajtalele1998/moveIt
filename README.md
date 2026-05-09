# MoveIt 🚀

MoveIt is a high-performance, hybrid file transfer platform that combines the speed of Peer-to-Peer (P2P) technology with the reliability of cloud-based streaming.

## 🌟 Key Features

### 1. Ultra-Fast P2P Transfers
Directly send files between devices using WebRTC. No middleman, no speed limits, and maximum privacy.

### 2. Hybrid Cloud API
For situations where P2P isn't possible or for third-party integrations, MoveIt provides a robust REST API that supports:
- **Streaming Uploads**: Handle massive files (up to 5GB) without RAM exhaustion.
- **Auto-Zipping**: Automatically bundles multiple files into a single ZIP archive.
- **QR Code Integration**: Instantly generate QR codes for mobile-to-desktop transfers.

### 3. Developer Platform (NEW)
MoveIt now supports third-party developers with:
- **API Key Authentication**: Secure your integrations with unique keys.
- **Usage Analytics**: Track your transfer volume and data usage in real-time.
- **Webhook Callbacks**: Get notified instantly when your transfers are completed.
- **Transfer Management**: Programmatically list, extend, or delete active transfers.

## 🛠️ Tech Stack
- **Frontend**: Vanilla JS, HTML5, CSS3 (Glassmorphism UI)
- **Backend**: Node.js, Express
- **Real-time**: Firebase Firestore (Signaling)
- **Storage**: Firebase Storage (API Transfers)
- **P2P**: WebRTC (RTCPeerConnection)

## 🚀 Getting Started

### Local Development
1. Clone the repository.
2. Install dependencies: `npm install`
3. Set up environment variables:
   - `FIREBASE_SERVICE_ACCOUNT`: Your Firebase Service Account JSON.
   - `FRONTEND_URL`: The URL where your frontend is hosted.
4. Run the dev server: `npm run dev`

### API Usage
See the [API Documentation](api_documentation.md) for detailed information on how to integrate MoveIt into your own apps.

## 📄 License
MIT License. Built for speed and simplicity.
