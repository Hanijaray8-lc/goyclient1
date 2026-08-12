const express = require('express');
const mongoose = require('mongoose');
const cors = require('cors');
const http = require('http'); 
const fs = require('fs');
const path = require('path');
const { Server } = require('socket.io'); // Socket.io
const { default: makeWASocket, useMultiFileAuthState, DisconnectReason, fetchLatestBaileysVersion } = require('@whiskeysockets/baileys');
const pino = require('pino');
require('dotenv').config();

const app = express();
const server = http.createServer(app);
const allowedOrigins = [
  "https://goyclient1.onrender.com/",
  "https://goyclient1.onrender.com/",
"https://wb.lcind.space",
  "https://goyee.lcind.space"
];

// Socket.io CORS setup
const io = new Server(server, {
  cors: {
    origin: allowedOrigins,
    methods: ["GET", "POST"],
    credentials: true
  },
  maxHttpBufferSize: 1e8 // 100 MB limit for videos
});

app.use(cors({
  origin: allowedOrigins,
  credentials: true
}));
app.use(express.json({ limit: '100mb' }));
app.use(express.urlencoded({ limit: '100mb', extended: true }));

// MongoDB connection
mongoose.connect(process.env.MONGO_URI)
  .then(async () => {
    console.log("Connected to MongoDB Atlas");
    // MongoDB TTL indexes don't auto-update their expireAfterSeconds just
    // because the schema changed — the index already exists in the DB from
    // before. This explicitly syncs it to whatever HISTORY_RETENTION_SECONDS
    // is currently set to (15 min for testing, 7 days for production).
    try {
      const result = await mongoose.connection.db.command({
        collMod: 'messagehistories',
        index: {
          keyPattern: { createdAt: 1 },
          expireAfterSeconds: HISTORY_RETENTION_SECONDS
        }
      });
      console.log(`✅ History TTL index synced to ${HISTORY_RETENTION_SECONDS}s`);
    } catch (err) {
      // Fails harmlessly on first-ever run before the collection/index exists —
      // mongoose will create it fresh with the correct value in that case.
      console.log("ℹ️ TTL index collMod skipped (likely first run / index not yet created). Full error:", err.message);
    }
  })
  .catch((err) => console.error("MongoDB error:", err));

// --- WhatsApp Logic Setup (User-Specific) ---
const userClients = {}; // key: email, value: { whatsappClient, latestQR, isWhatsAppAuthenticated }
const socketToEmail = {}; // key: socket.id, value: email
const emailToSockets = {}; // key: email, value: array of socket.id

function emitToUserSockets(email, event, data) {
    const socketIds = emailToSockets[email];
    if (socketIds) {
        socketIds.forEach(id => {
            io.to(id).emit(event, data);
        });
    }
}

async function startWhatsAppForUser(email) {
    if (!email) return;
    
    if (userClients[email]) {
        return userClients[email];
    }

    // Set loading/placeholder immediately to prevent concurrent startWhatsAppForUser race conditions
    userClients[email] = {
        whatsappClient: null,
        latestQR: "",
        isWhatsAppAuthenticated: false,
        loading: true
    };

    try {
        const folderName = `auth_info_${email.replace(/[^a-zA-Z0-9]/g, '_')}`;
        const { state, saveCreds } = await useMultiFileAuthState(folderName);
        const { version } = await fetchLatestBaileysVersion();

        const client = makeWASocket({
            version,
            auth: state,
            printQRInTerminal: false,
            logger: pino({ level: "silent" }),
            browser: ["Goyee", "Chrome", "1.0.0"]
        });

        userClients[email].whatsappClient = client;
        userClients[email].loading = false;

        client.ev.on('creds.update', saveCreds);

    client.ev.on('connection.update', async (update) => {
        const { connection, lastDisconnect, qr } = update;

        if (qr) {
            if (userClients[email]) {
                userClients[email].latestQR = qr;
                emitToUserSockets(email, "qr", qr);
            }
        }

        if (connection === 'open') {
            if (userClients[email]) {
                const scannedDigits = client.user?.id ? client.user.id.split(':')[0].replace(/\D/g, '').slice(-10) : '';

                let registeredDigits = '';
                try {
                    const dbUser = await User.findOne({ email }, 'phone').lean();
                    registeredDigits = dbUser && dbUser.phone ? String(dbUser.phone).replace(/\D/g, '').slice(-10) : '';
                } catch (e) {
                    console.error(`Could not look up registered phone for ${email}:`, e.message);
                }

                // Reject the session if the scanned WhatsApp number doesn't
                // match the account's registered mobile number. (If no phone
                // is on file for this account, we can't enforce this check —
                // make the registered phone required at signup if this must
                // always be enforced.)
                if (registeredDigits && scannedDigits && registeredDigits !== scannedDigits) {
                    console.warn(`⚠️ WhatsApp number mismatch for ${email}: registered ending ${registeredDigits}, scanned ending ${scannedDigits}. Rejecting session.`);
                    emitToUserSockets(email, "number_mismatch", {
                        message: `The WhatsApp number you scanned doesn't match your registered mobile number. Please log out and scan again using the number ending in ${registeredDigits}.`,
                        registeredNumber: registeredDigits,
                        scannedNumber: scannedDigits
                    });
                    userClients[email].latestQR = "";
                    userClients[email].isWhatsAppAuthenticated = false;
                    try { await client.logout(); } catch (e) { /* ignore */ }
                    const authPath = path.join(__dirname, folderName);
                    if (fs.existsSync(authPath)) {
                        try { fs.rmSync(authPath, { recursive: true, force: true }); } catch (e) { /* ignore */ }
                    }
                    delete userClients[email];
                    return;
                }

                console.log(`✅ WhatsApp Authenticated for ${email}!`);
                userClients[email].latestQR = ""; 
                userClients[email].isWhatsAppAuthenticated = true;
                
                const userInfo = client.user ? {
                    id: scannedDigits,
                    name: client.user.name || client.user.verifiedName || ''
                } : null;
                
                emitToUserSockets(email, "ready", { message: "WhatsApp Authenticated Successfully!", user: userInfo });
            }
        }

        if (connection === 'close') {
            const shouldReconnect = lastDisconnect.error?.output?.statusCode !== DisconnectReason.loggedOut;
            console.log(`❌ WhatsApp connection closed for ${email}. Reconnecting:`, shouldReconnect);
            
            if (shouldReconnect) {
                delete userClients[email];
                startWhatsAppForUser(email);
            } else {
                if (userClients[email]) {
                    userClients[email].latestQR = ""; 
                    userClients[email].isWhatsAppAuthenticated = false;
                    emitToUserSockets(email, "logout", "User logged out from phone");
                }
                const authPath = path.join(__dirname, folderName);
                if (fs.existsSync(authPath)) {
                    try { fs.rmSync(authPath, { recursive: true, force: true }); } catch(e) {}
                }
                delete userClients[email];
                startWhatsAppForUser(email);
            }
        }
    });
  } catch (err) {
      console.error(`Error starting WhatsApp for ${email}:`, err);
      delete userClients[email];
  }
}

// --- Socket Connection ---
io.on("connection", (socket) => {
    console.log("React UI Connected to Socket!");

    socket.on("register_email", async (email) => {
        if (!email) return;
        
        socketToEmail[socket.id] = email;
        if (!emailToSockets[email]) {
            emailToSockets[email] = [];
        }
        if (!emailToSockets[email].includes(socket.id)) {
            emailToSockets[email].push(socket.id);
        }

        console.log(`Socket ${socket.id} registered for email: ${email}`);
        await startWhatsAppForUser(email);

        const userClient = userClients[email];
        if (userClient) {
            if (userClient.isWhatsAppAuthenticated) {
                const client = userClient.whatsappClient;
                const userInfo = client && client.user ? {
                    id: client.user.id ? client.user.id.split(':')[0] : '',
                    name: client.user.name || client.user.verifiedName || ''
                } : null;
                socket.emit("ready", { message: "WhatsApp Authenticated Successfully!", user: userInfo });
            } else if (userClient.latestQR) {
                socket.emit("qr", userClient.latestQR);
            }
        }
    });

    socket.on("disconnect", () => {
        const email = socketToEmail[socket.id];
        if (email) {
            if (emailToSockets[email]) {
                emailToSockets[email] = emailToSockets[email].filter(id => id !== socket.id);
                if (emailToSockets[email].length === 0) {
                    delete emailToSockets[email];
                }
            }
            delete socketToEmail[socket.id];
        }
        console.log("Socket disconnected:", socket.id);
    });

    socket.on("check_status", () => {
        const email = socketToEmail[socket.id];
        if (!email) return;
        const userClient = userClients[email];
        if (userClient) {
            if (userClient.isWhatsAppAuthenticated) {
                const client = userClient.whatsappClient;
                const userInfo = client && client.user ? {
                    id: client.user.id ? client.user.id.split(':')[0] : '',
                    name: client.user.name || client.user.verifiedName || ''
                } : null;
                socket.emit("ready", { message: "WhatsApp Authenticated Successfully!", user: userInfo });
            } else if (userClient.latestQR) {
                socket.emit("qr", userClient.latestQR); 
            }
        }
    });

    let forceResetting = false;
    socket.on("request_new_qr", async () => {
        const email = socketToEmail[socket.id];
        if (!email) return;

        if (forceResetting) return;
        forceResetting = true;
        
        console.log(`♻️ Force restarting WhatsApp to generate new QR for ${email}...`);
        const userClient = userClients[email];
        if (userClient) {
            userClient.isWhatsAppAuthenticated = false;
            userClient.latestQR = "";
            if (userClient.whatsappClient) {
                userClient.whatsappClient.ev.removeAllListeners('connection.update');
                try { userClient.whatsappClient.end(new Error("Force reset")); } catch(e) {}
            }
        }
        
        setTimeout(() => {
            const folderName = `auth_info_${email.replace(/[^a-zA-Z0-9]/g, '_')}`;
            const authPath = path.join(__dirname, folderName);
            if (fs.existsSync(authPath)) {
                try { fs.rmSync(authPath, { recursive: true, force: true }); } catch(e) {}
            }
            delete userClients[email];
            startWhatsAppForUser(email);
            forceResetting = false;
        }, 2000);
    });

    socket.on("send_bulk_message", async (data, callback) => {
        const email = socketToEmail[socket.id];
        const userClient = userClients[email];
        
        if (!userClient || !userClient.isWhatsAppAuthenticated) {
            if (typeof callback === "function") {
                callback({ success: false, error: "WhatsApp not authenticated" });
            }
            return;
        }

        const whatsappClient = userClient.whatsappClient;
        const { numbers, text, media } = data;
        console.log("📥 Received numbers from frontend:", numbers);
        
        let validNumbersCount = 0;
        let successCount = 0;
        let failedCount = 0;

        const deviceName = whatsappClient && whatsappClient.user
            ? (whatsappClient.user.name || whatsappClient.user.verifiedName || (whatsappClient.user.id ? whatsappClient.user.id.split(':')[0] : ''))
            : '';
        const historyRecords = []; // collected here, written once via insertMany to avoid duplicate records

        socket.emit("bulk_progress_start", { total: numbers.length });

        for (const num of numbers) {
            if (num && String(num).trim() !== "") {
                validNumbersCount++;
                let cleanNum = String(num).replace(/\D/g, '');
                if (cleanNum.length === 10) {
                    cleanNum = '91' + cleanNum;
                }
                const formattedNumber = `${cleanNum}@s.whatsapp.net`;
                
                socket.emit("bulk_progress_update", { status: "sending", number: num });
                
                try {
                    // Check if the number is actually registered on WhatsApp
                    const checkNumber = await whatsappClient.onWhatsApp(formattedNumber);
                    if (!checkNumber || checkNumber.length === 0 || !checkNumber[0].exists) {
                        console.error(`❌ Number ${num} is not registered on WhatsApp`);
                        socket.emit("bulk_progress_update", { status: "failed", number: num, reason: "Number is not registered on WhatsApp" });
                        failedCount++;
                        continue; // Skip sending, move to next number
                    }

                    if (media && media.data) {
                        const buffer = Buffer.from(media.data, 'base64');
                        let mediaMessage = {};
                        let mime = media.mimetype ? media.mimetype.toLowerCase() : '';
                        const name = media.filename ? media.filename.toLowerCase() : '';
                        
                        // Fallback mime from file extension if empty or generic octet-stream
                        if (!mime || mime === 'application/octet-stream') {
                            const ext = name.split('.').pop();
                            const mimeMap = {
                                'png': 'image/png',
                                'jpg': 'image/jpeg',
                                'jpeg': 'image/jpeg',
                                'gif': 'image/gif',
                                'webp': 'image/webp',
                                'mp4': 'video/mp4',
                                'mov': 'video/quicktime',
                                'avi': 'video/x-msvideo',
                                'pdf': 'application/pdf',
                                'csv': 'text/csv',
                                'xls': 'application/vnd.ms-excel',
                                'xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                                'doc': 'application/msword',
                                'docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
                                'txt': 'text/plain'
                            };
                            mime = mimeMap[ext] || mime;
                        }

                        const isImage = mime.startsWith('image/') || name.match(/\.(jpg|jpeg|png|gif|webp)$/);
                        const isVideo = mime.startsWith('video/') || name.match(/\.(mp4|mov|avi|webm|mkv)$/);

                        if (isImage) {
                            mediaMessage = { image: buffer, caption: text, mimetype: mime || 'image/jpeg' };
                        } else if (isVideo) {
                            mediaMessage = { video: buffer, caption: text, mimetype: mime || 'video/mp4' };
                        } else {
                            mediaMessage = { document: buffer, caption: text, mimetype: mime || 'application/octet-stream', fileName: media.filename || 'document' };
                        }
                        await whatsappClient.sendMessage(formattedNumber, mediaMessage);
                    } else {
                        await whatsappClient.sendMessage(formattedNumber, { text: text });
                    }
                    console.log(`✅ Message sent to ${num}`);
                    socket.emit("bulk_progress_update", { status: "sent", number: num });
                    successCount++;
                    historyRecords.push({ email, deviceName, phone: String(num), message: text || (media ? `[${media.filename || 'attachment'}]` : ''), status: 'Sent', sentAt: new Date() });
                    await new Promise(resolve => setTimeout(resolve, 2000));
                } catch (error) {
                    console.error(`❌ Failed to send to ${num}`, error);
                    
                    let reason = error.message || "Unable to send message. Please try again.";
                    
                    // Map raw exceptions to user-friendly messages
                    if (reason.includes("reading 'id'") || reason.includes("undefined") || reason.includes("Cannot read properties")) {
                        reason = "Number is not registered on WhatsApp.";
                    } else if (reason.includes("Unexpected error")) {
                        reason = "Unable to send message. Please try again.";
                    }

                    socket.emit("bulk_progress_update", { status: "failed", number: num, reason: reason });
                    failedCount++;
                    historyRecords.push({ email, deviceName, phone: String(num), message: text || (media ? `[${media.filename || 'attachment'}]` : ''), status: 'Failed', reason, sentAt: new Date() });
                }
            } else {
                if (num) {
                    socket.emit("bulk_progress_update", { status: "failed", number: num, reason: "Invalid empty number" });
                    failedCount++;
                }
            }
        }

        // Always increment the analytics count for the dashboard based on valid numbers submitted
        if (validNumbersCount > 0 || failedCount > 0) {
            const d = new Date();
            const offset = d.getTimezoneOffset() * 60000;
            const today = new Date(d.getTime() - offset).toISOString().split('T')[0];
            
            await MessageLog.findOneAndUpdate(
                { date: today },
                { $inc: { count: validNumbersCount, successCount: successCount, failedCount: failedCount } },
                { upsert: true, new: true }
            );
        }

        // Record each recipient's send attempt into History (one write per bulk send, not per number, so no duplicates)
        if (historyRecords.length > 0) {
            try {
                await MessageHistory.insertMany(historyRecords, { ordered: false });
            } catch (histErr) {
                console.error("⚠️ Failed to save message history:", histErr.message);
            }
        }

        socket.emit("bulk_progress_completed", { success: true });

        if (typeof callback === "function") {
            callback({ success: true });
        }
    });
});




const schedule = require('node-schedule');
const Schedule = require('./models/Schedule');

app.get('/api/schedules', async (req, res) => {
    try {
        const schedules = await Schedule.find().sort({ scheduledFor: 1 });
        res.json(schedules);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

app.post('/api/schedules', async (req, res) => {
    try {
        const newSchedule = new Schedule(req.body);
        await newSchedule.save();
        scheduleJob(newSchedule);
        res.json({ message: 'Scheduled successfully', schedule: newSchedule });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

app.put('/api/schedules/:id', async (req, res) => {
    try {
        const updated = await Schedule.findByIdAndUpdate(req.params.id, req.body, { new: true });
        const existingJob = schedule.scheduledJobs[updated._id.toString()];
        if (existingJob) existingJob.cancel();
        
        if (updated.status === 'Pending') {
            scheduleJob(updated);
        }
        res.json(updated);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

app.delete('/api/schedules/:id', async (req, res) => {
    try {
        await Schedule.findByIdAndDelete(req.params.id);
        const existingJob = schedule.scheduledJobs[req.params.id];
        if (existingJob) existingJob.cancel();
        res.json({ message: 'Deleted' });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

function scheduleJob(scheduleDoc) {
    if (scheduleDoc.status !== 'Pending') return;
    
    schedule.scheduleJob(scheduleDoc._id.toString(), new Date(scheduleDoc.scheduledFor), async () => {
        try {
            console.log('Running scheduled job for', scheduleDoc._id);
            const email = scheduleDoc.email;
            const userClient = userClients[email];
            if (!userClient || !userClient.isWhatsAppAuthenticated) {
                console.log(`Cannot send scheduled message: WhatsApp not authenticated for ${email}`);
                await Schedule.findByIdAndUpdate(scheduleDoc._id, { status: 'Failed' });
                return;
            }
            
            const whatsappClient = userClient.whatsappClient;
            let validNumbersCount = 0;
            let successCount = 0;
            let failedCount = 0;
            const deviceName = whatsappClient && whatsappClient.user
                ? (whatsappClient.user.name || whatsappClient.user.verifiedName || (whatsappClient.user.id ? whatsappClient.user.id.split(':')[0] : ''))
                : '';
            const historyRecords = [];
 
            io.emit("bulk_progress_start", { total: scheduleDoc.contacts.length });
 
            for (let num of scheduleDoc.contacts) {
                if (!num || String(num).trim() === "") {
                    io.emit("bulk_progress_update", { status: "failed", number: num, reason: "Invalid empty number" });
                    failedCount++;
                    continue;
                }
 
                validNumbersCount++;
                let cleanNum = String(num).replace(/\D/g, '');
                if (cleanNum.length === 10) {
                    cleanNum = '91' + cleanNum;
                }
                const formattedNumber = `${cleanNum}@s.whatsapp.net`;
 
                io.emit("bulk_progress_update", { status: "sending", number: num });
 
                try {
                    const checkNumber = await whatsappClient.onWhatsApp(formattedNumber);
                    if (!checkNumber || checkNumber.length === 0 || !checkNumber[0].exists) {
                        io.emit("bulk_progress_update", { status: "failed", number: num, reason: "Number is not registered on WhatsApp" });
                        failedCount++;
                        continue;
                    }
 
                    if (scheduleDoc.media && scheduleDoc.media.data) {
                        const buffer = Buffer.from(scheduleDoc.media.data, 'base64');
                        let mediaMessage = {};
                        let mime = scheduleDoc.media.mimetype ? scheduleDoc.media.mimetype.toLowerCase() : '';
                        const name = scheduleDoc.media.filename ? scheduleDoc.media.filename.toLowerCase() : '';
                        
                        // Fallback mime from file extension if empty or generic octet-stream
                        if (!mime || mime === 'application/octet-stream') {
                            const ext = name.split('.').pop();
                            const mimeMap = {
                                'png': 'image/png',
                                'jpg': 'image/jpeg',
                                'jpeg': 'image/jpeg',
                                'gif': 'image/gif',
                                'webp': 'image/webp',
                                'mp4': 'video/mp4',
                                'mov': 'video/quicktime',
                                'avi': 'video/x-msvideo',
                                'pdf': 'application/pdf',
                                'csv': 'text/csv',
                                'xls': 'application/vnd.ms-excel',
                                'xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                                'doc': 'application/msword',
                                'docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
                                'txt': 'text/plain'
                            };
                            mime = mimeMap[ext] || mime;
                        }
 
                        const isImage = mime.startsWith('image/') || name.match(/\.(jpg|jpeg|png|gif|webp)$/);
                        const isVideo = mime.startsWith('video/') || name.match(/\.(mp4|mov|avi|webm|mkv)$/);
 
                        if (isImage) {
                            mediaMessage = { image: buffer, caption: scheduleDoc.message, mimetype: mime || 'image/jpeg' };
                        } else if (isVideo) {
                            mediaMessage = { video: buffer, caption: scheduleDoc.message, mimetype: mime || 'video/mp4' };
                        } else {
                            mediaMessage = { document: buffer, caption: scheduleDoc.message, mimetype: mime || 'application/octet-stream', fileName: scheduleDoc.media.filename || 'document' };
                        }
                        await whatsappClient.sendMessage(formattedNumber, mediaMessage);
                    } else {
                        await whatsappClient.sendMessage(formattedNumber, { text: scheduleDoc.message });
                    }
                    io.emit("bulk_progress_update", { status: "sent", number: num });
                    successCount++;
                    historyRecords.push({ email, deviceName, phone: String(num), message: scheduleDoc.message || (scheduleDoc.media ? `[${scheduleDoc.media.filename || 'attachment'}]` : ''), status: 'Sent', sentAt: new Date() });
                    await new Promise(resolve => setTimeout(resolve, 2000));
                } catch(e) {
                    console.error('Error sending scheduled to', num, e);
                    let reason = e.message || "Unable to send message.";
                    if (reason.includes("reading 'id'") || reason.includes("undefined") || reason.includes("Cannot read properties")) {
                        reason = "Number is not registered on WhatsApp.";
                    }
                    io.emit("bulk_progress_update", { status: "failed", number: num, reason: reason });
                    failedCount++;
                    historyRecords.push({ email, deviceName, phone: String(num), message: scheduleDoc.message || (scheduleDoc.media ? `[${scheduleDoc.media.filename || 'attachment'}]` : ''), status: 'Failed', reason, sentAt: new Date() });
                }
            }
 
            if (validNumbersCount > 0 || failedCount > 0) {
                const d = new Date();
                const offset = d.getTimezoneOffset() * 60000;
                const today = new Date(d.getTime() - offset).toISOString().split('T')[0];
                await MessageLog.findOneAndUpdate(
                    { date: today },
                    { $inc: { count: validNumbersCount, successCount: successCount, failedCount: failedCount } },
                    { upsert: true, new: true }
                );
            }

            if (historyRecords.length > 0) {
                try {
                    await MessageHistory.insertMany(historyRecords, { ordered: false });
                } catch (histErr) {
                    console.error("⚠️ Failed to save message history (scheduled):", histErr.message);
                }
            }
 
            io.emit("bulk_progress_completed", { success: true });
            
            // If completely failed
            if (successCount === 0 && failedCount > 0) {
                 await Schedule.findByIdAndUpdate(scheduleDoc._id, { status: 'Failed', sentAt: new Date() });
            } else {
                 await Schedule.findByIdAndUpdate(scheduleDoc._id, { status: 'Completed', sentAt: new Date() });
            }
        } catch(e) {
            console.error('Job error', e);
            io.emit("bulk_progress_completed", { success: false });
            await Schedule.findByIdAndUpdate(scheduleDoc._id, { status: 'Failed' });
        }
    });
}
 
mongoose.connection.once('open', async () => {
    const pendings = await Schedule.find({ status: 'Pending' });
    for (const s of pendings) {
        if (s.email) {
            await startWhatsAppForUser(s.email);
        }
        scheduleJob(s);
    }
});

const PORT = process.env.PORT || 5000;
// ==========================================
// USER LOGIN & REGISTER API ROUTES
// ==========================================

const userSchema = new mongoose.Schema({
  name: { type: String, required: true },
  email: { type: String, required: true, unique: true },
  password: { type: String, required: true },
  phone: { type: String, required: false },
  location: { type: String, required: false },
  totalSent: { type: Number, default: 0 }
});

const messageLogSchema = new mongoose.Schema({
  date: { type: String, required: true, unique: true }, // Format: YYYY-MM-DD
  count: { type: Number, default: 0 },
  successCount: { type: Number, default: 0 },
  failedCount: { type: Number, default: 0 }
});

// --- Message History (auto-expiring log of every send attempt) ---
// ⚙️ TESTING MODE: retention is currently 15 MINUTES instead of 7 days.
// To restore production behavior, comment out the "testing" line and
// uncomment the "production" line below — nothing else needs to change,
// since everything downstream just reads HISTORY_RETENTION_SECONDS.
// const HISTORY_RETENTION_SECONDS = 15 * 60;        // ← TESTING: 15 minutes
const HISTORY_RETENTION_SECONDS = 7 * 24 * 60 * 60; // ← PRODUCTION: 7 days

const messageHistorySchema = new mongoose.Schema({
  email: { type: String, required: true }, // user/device identifier
  deviceName: { type: String },   // connected WhatsApp device/user info, if available
  phone: { type: String, required: true },
  message: { type: String, default: "" },
  status: { type: String, enum: ['Sent', 'Failed'], required: true },
  reason: { type: String },       // failure reason, if any
  sentAt: { type: Date, default: Date.now },
  createdAt: { type: Date, default: Date.now }
});
// Single compound index used for both lookups and the 7-day TTL cleanup —
// avoids defining two separate indexes on overlapping fields, which is
// what causes an IndexOptionsConflict if a collection already exists.
messageHistorySchema.index({ email: 1, createdAt: -1 });
messageHistorySchema.index({ createdAt: 1 }, { expireAfterSeconds: HISTORY_RETENTION_SECONDS });

// Avoid OverwriteModelError if it's already defined
const User = mongoose.models.User || mongoose.model('User', userSchema);
const MessageLog = mongoose.models.MessageLog || mongoose.model('MessageLog', messageLogSchema);
const MessageHistory = mongoose.models.MessageHistory || mongoose.model('MessageHistory', messageHistorySchema);

// --- Backup cleanup loop ---
// MongoDB's own TTL background thread handles deletion automatically, but it
// only sweeps roughly once every 60 seconds and can occasionally lag,
// especially right after an index's expireAfterSeconds value has just been
// changed (as during this 15-minute testing setup). This app-level interval
// runs the exact same deletion explicitly and logs what it removes, so you
// can see and trust that cleanup is actually happening — it never touches
// User.totalSent, so the Overall Message Count is always unaffected.
setInterval(async () => {
  try {
    const cutoff = new Date(Date.now() - HISTORY_RETENTION_SECONDS * 1000);
    const result = await MessageHistory.deleteMany({ createdAt: { $lt: cutoff } });
    if (result.deletedCount > 0) {
      console.log(`🧹 Backup cleanup: removed ${result.deletedCount} expired history record(s) older than ${HISTORY_RETENTION_SECONDS}s`);
    }
  } catch (err) {
    console.error("Backup history cleanup error:", err.message);
  }
}, 60 * 1000); // check every 60 seconds

// Surface index-build errors (e.g. a conflicting index left over from an
// earlier run) instead of letting them fail silently in the background.
MessageHistory.on('index', (err) => {
  if (err) {
    console.error('❌ MessageHistory index build failed:', err.message);
    console.error('   If this mentions "IndexOptionsConflict" or "IndexKeySpecsConflict", drop the old index once with:');
    console.error('   db.messagehistories.dropIndexes()  (run in mongosh / MongoDB Compass), then restart the server.');
  }
});

app.post('/api/register', async (req, res) => {
  try {
    const { name, email, password, phone, location } = req.body;
    const existingUser = await User.findOne({ email });
    if (existingUser) return res.status(400).json({ message: "User already exists" });

    const newUser = new User({ name, email, password, phone, location });

    await newUser.save();
    res.status(201).json({ message: "Registration successful!" });
  } catch (error) {
    res.status(500).json({ message: "Error registering user", error });
  }
});

app.post('/api/login', async (req, res) => {
  try {
    const { email, password } = req.body;
    const user = await User.findOne({ email, password });
    if (!user) return res.status(401).json({ message: "Invalid email or password" });

    res.status(200).json({
      message: "Login successful!",
      name: user.name,
      email: user.email,
      phone: user.phone || "",
      location: user.location || "",
      totalSent: user.totalSent || 0
    });
  } catch (error) {
    res.status(500).json({ message: "Error logging in", error });
  }
});

app.get('/api/users', async (req, res) => {
  try {
    const users = await User.find({}, '-password').sort({ _id: -1 }).lean(); // Exclude passwords, sort descending, use lean for speed
    res.status(200).json(users);
  } catch (error) {
    res.status(500).json({ message: "Error fetching users", error });
  }
});

app.delete('/api/users/:id', async (req, res) => {
  try {
    const deletedUser = await User.findByIdAndDelete(req.params.id);
    if (!deletedUser) return res.status(404).json({ message: "User not found" });
    res.status(200).json({ message: "User deleted successfully" });
  } catch (error) {
    res.status(500).json({ message: "Error deleting user", error });
  }
});

app.get('/api/messages/stats', async (req, res) => {
  try {
    const stats = await MessageLog.find().sort({ date: 1 });
    res.status(200).json(stats);
  } catch (error) {
    res.status(500).json({ message: "Error fetching message stats", error });
  }
});

app.post('/api/user/increment-sent', async (req, res) => {
  try {
    const { email } = req.body;
    let user = await User.findOne({ email });
    if (user) {
      user.totalSent = (user.totalSent || 0) + 1;
      await user.save();
      res.status(200).json({ totalSent: user.totalSent });
    } else {
      res.status(404).json({ message: "User not found" });
    }
  } catch (error) {
    res.status(500).json({ message: "Error incrementing sent", error });
  }
});

// GET /api/user/total-sent?email=user@example.com
// Returns the permanent, cumulative "Overall Message Count" for a user.
// This value lives on the User document (not MessageHistory), so it is
// never affected by the 7-day TTL cleanup of history records.
app.get('/api/user/total-sent', async (req, res) => {
  try {
    const { email } = req.query;
    if (!email) return res.status(400).json({ message: "email is required" });

    const user = await User.findOne({ email }, 'totalSent').lean();
    if (!user) return res.status(404).json({ message: "User not found" });

    res.status(200).json({ totalSent: user.totalSent || 0 });
  } catch (error) {
    res.status(500).json({ message: "Error fetching total sent", error: error.message });
  }
});

// ==========================================
// MESSAGE HISTORY API ROUTES (7-day retention, auto-cleaned via MongoDB TTL index)
// ==========================================

// GET /api/history?email=user@example.com&search=9876543210&status=Sent
app.get('/api/history', async (req, res) => {
  try {
    const { email, search, status } = req.query;
    if (!email) return res.status(400).json({ message: "email is required" });

    const query = { email };
    if (status && (status === 'Sent' || status === 'Failed')) {
      query.status = status;
    }
    if (search && String(search).trim() !== "") {
      const s = String(search).trim();
      query.$or = [
        { phone: { $regex: s, $options: 'i' } },
        { message: { $regex: s, $options: 'i' } }
      ];
    }

    const records = await MessageHistory.find(query).sort({ createdAt: -1 }).lean();
    res.status(200).json({ total: records.length, records });
  } catch (error) {
    console.error('❌ /api/history error:', error);
    res.status(500).json({ message: "Error fetching message history", error: error.message });
  }
});

// ==========================================
// CONTACT FORM WHATSAPP ROUTE (UPDATED)
// ==========================================

app.post('/api/contact/whatsapp', async (req, res) => {
  try {
    const { name, email, subject, message, text } = req.body;

    // --- validation (unchanged) ---
    const validationErrors = [];
    if (!name || !String(name).trim()) validationErrors.push('Please enter your full name.');
    if (!email || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(String(email))) validationErrors.push('Please enter a valid email address.');
    if (!subject || !String(subject).trim()) validationErrors.push('Please enter a subject.');
    if (!message || !String(message).trim()) validationErrors.push('Please enter your message.');

    if (validationErrors.length > 0) {
      return res.status(400).json({ message: validationErrors[0] });
    }

    // --- NEW: get the client for this specific user using the email ---
    const userClient = userClients[email];
    if (!userClient || !userClient.isWhatsAppAuthenticated || !userClient.whatsappClient) {
      return res.status(403).json({
        message: 'Your WhatsApp is not connected. Please connect your WhatsApp first before sending a message.'
      });
    }
    const activeClient = userClient.whatsappClient;

    // --- send to admin (unchanged) ---
    const destination = String(process.env.WHATSAPP_CONTACT_NUMBER || '919486042369').replace(/\D/g, '');
    if (!destination) {
      return res.status(500).json({ message: 'WhatsApp destination is not configured.' });
    }

    const formattedNumber = destination.startsWith('91') ? `${destination}@s.whatsapp.net` : `91${destination}@s.whatsapp.net`;
    const payload = {
      text: text || `New contact form submission\nName: ${name}\nEmail: ${email}\nSubject: ${subject}\nMessage: ${message}`
    };

    await activeClient.sendMessage(formattedNumber, payload);
    res.status(200).json({ message: 'Contact message sent successfully.' });
  } catch (error) {
    console.error('Contact WhatsApp error:', error);
    res.status(500).json({ message: 'Unable to send your message right now.' });
  }
});

// --- Support Request / Feedback API ---
const supportSchema = new mongoose.Schema({
  username: { type: String },
  email: { type: String },
  type: { type: String, required: true }, // 'issue' or 'feedback'
  issueType: { type: String },
  description: { type: String, required: true },
  rating: { type: Number },
  screenshot: {
    filename: { type: String },
    mimetype: { type: String },
    data: { type: String } // base64 string
  },
  createdAt: { type: Date, default: Date.now }
});

const Support = mongoose.models.Support || mongoose.model('Support', supportSchema);

app.post('/api/support', async (req, res) => {
  try {
    const { username, email, type, issueType, description, rating, screenshot } = req.body;

    const supportDoc = new Support({
      username: username || "Guest",
      email: email || "no-email@goyee.com",
      type,
      issueType,
      description,
      rating,
      screenshot
    });
    await supportDoc.save();

    const destination = '919943042369'; 


    let cleanDest = String(destination).replace(/\D/g, '');
    if (cleanDest.length === 10) {
      cleanDest = '91' + cleanDest;
    }
    const formattedNumber = `${cleanDest}@s.whatsapp.net`;

    const dateFormatted = new Date().toLocaleDateString('en-US', { day: '2-digit', month: 'short', year: 'numeric' });
    const timeFormatted = new Date().toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', hour12: true });

    let textMessage = '';
    if (type === 'issue') {
      textMessage = `New Help Request\n\nUser:\n${username}\n\nEmail:\n${email}\n\nIssue:\n${issueType}\n\nDescription:\n${description}\n\nDate:\n${dateFormatted}\n${timeFormatted}`;
    } else {
      const stars = '⭐'.repeat(rating || 0);
      textMessage = `New User Feedback\n\nUser:\n${username}\n\n${stars}\n\nFeedback:\n${description}`;
    }

    let userClient = userClients[email] || Object.values(userClients).find(uc => uc.isWhatsAppAuthenticated);
    
    // If the client is currently loading or reconnecting, wait up to 10 seconds for it to become ready
    const maxRetries = 10;
    let retries = 0;
    while (retries < maxRetries && (!userClient || !userClient.isWhatsAppAuthenticated || !userClient.whatsappClient || userClient.loading)) {
      console.log(`⏳ [Support API] Waiting for WhatsApp client to be ready for ${email || "Guest"} (attempt ${retries + 1}/${maxRetries})...`);
      await new Promise(resolve => setTimeout(resolve, 1000));
      retries++;
      userClient = userClients[email] || Object.values(userClients).find(uc => uc.isWhatsAppAuthenticated);
    }

    if (!userClient || !userClient.isWhatsAppAuthenticated || !userClient.whatsappClient) {
      console.error(`❌ [Support API] WhatsApp client not authenticated/ready after waiting.`);
      return res.status(503).json({ message: 'WhatsApp is not authenticated or ready yet. Please connect your device.' });
    }

    const whatsappClient = userClient.whatsappClient;

    console.log(`🔎 [Support API] Checking if admin number ${cleanDest} exists on WhatsApp...`);
    let numberExists = false;
    try {
      const checkNumber = await whatsappClient.onWhatsApp(formattedNumber);
      if (checkNumber && checkNumber.length > 0 && checkNumber[0].exists) {
        numberExists = true;
      }
    } catch (checkErr) {
      console.warn(`⚠️ [Support API] Failed to verify number on WhatsApp, proceeding anyway:`, checkErr.message);
      numberExists = true; // Fallback in case of verification API failure
    }

    if (!numberExists) {
      console.error(`❌ [Support API] Admin number ${cleanDest} is not registered on WhatsApp.`);
      return res.status(400).json({ message: `Admin number ${cleanDest} is not registered on WhatsApp.` });
    }

    try {
      console.log(`📤 [Support API] Dispatching message to admin ${cleanDest}...`);
      let sendResult;
      if (screenshot && screenshot.data) {
        const buffer = Buffer.from(screenshot.data, 'base64');
        sendResult = await whatsappClient.sendMessage(formattedNumber, {
          image: buffer,
          caption: textMessage,
          mimetype: screenshot.mimetype || 'image/png'
        });
      } else {
        sendResult = await whatsappClient.sendMessage(formattedNumber, { text: textMessage });
      }

      console.log(`✅ [Support API] Support message successfully delivered. Message ID:`, sendResult?.key?.id || "N/A");
      return res.status(200).json({ message: 'Support request submitted successfully.' });
    } catch (waError) {
      console.error('❌ [Support API] Failed to send WhatsApp message to admin:', waError);
      return res.status(500).json({ message: 'Failed to send WhatsApp message to admin.', error: waError.message });
    }
  } catch (error) {
    console.error('Support API Error:', error);
    return res.status(500).json({ message: 'Error submitting support request', error: error.message });
  }
});

app.use((err, req, res, next) => {
    console.error("Express Error:", err);
    res.status(err.status || 500).json({ error: err.message || "Internal Server Error" });
});

server.listen(PORT, () => console.log(`Server running on port ${PORT}`));
