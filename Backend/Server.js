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
const { scrapeGoogleMapsLeads } = require("./scraper");


const app = express();
const server = http.createServer(app);
// Socket.io CORS setup
const io = new Server(server, {
  cors: { origin: "http://localhost:3000", methods: ["GET", "POST"] },
  maxHttpBufferSize: 1e8 // 100 MB limit for videos
});

app.use(cors());
app.use(express.json({ limit: '100mb' }));
app.use(express.urlencoded({ limit: '100mb', extended: true }));

const teamCampaignRoutes = require('./routes/teamCampaignRoutes');
app.use('/api/team-campaign', teamCampaignRoutes);

// Web Scraper API Endpoint
app.post("/api/scrape-leads", async (req, res) => {
  try {
    const { query, location } = req.body;
    if (!query) {
      return res.status(400).json({ success: false, message: "Query string is required" });
    }

    console.log(`🌐 API request received for query: "${query}" in "${location || 'Any'}"`);
    const leads = await scrapeGoogleMapsLeads(query, location || "");
    return res.json({ success: true, count: leads.length, leads });
  } catch (err) {
    console.error("Scraper API Route Error:", err);
    return res.status(500).json({ success: false, message: "Failed to scrape leads" });
  }
});

// MongoDB connection
mongoose.connect(process.env.MONGO_URI)
  .then(() => console.log("Connected to MongoDB Atlas"))
  .catch((err) => console.error("MongoDB error:", err));

// --- WhatsApp Logic Setup (User-Specific) ---
const userClients = {}; // key: email, value: { whatsappClient, latestQR, isWhatsAppAuthenticated }
const socketToEmail = {}; // key: socket.id, value: email
const emailToSockets = {}; // key: email, value: array of socket.id

app.set('userClients', userClients);
app.set('emitToUserSockets', emitToUserSockets);

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
        let version = [2, 3000, 1015901307];
        try {
            const vRes = await Promise.race([
                fetchLatestBaileysVersion(),
                new Promise((_, reject) => setTimeout(() => reject(new Error('Timeout')), 1500))
            ]);
            if (vRes && vRes.version) version = vRes.version;
        } catch(e) {}

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

    client.ev.on('connection.update', (update) => {
        const { connection, lastDisconnect, qr } = update;

        if (qr) {
            if (userClients[email]) {
                userClients[email].latestQR = qr;
                emitToUserSockets(email, "qr", qr);
            }
        }

        if (connection === 'open') {
            console.log(`✅ WhatsApp Authenticated for ${email}!`);
            if (userClients[email]) {
                userClients[email].latestQR = ""; 
                userClients[email].isWhatsAppAuthenticated = true;
                
                const userInfo = client.user ? {
                    id: client.user.id ? client.user.id.split(':')[0] : '',
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

                    const mediaList = Array.isArray(media) ? media : (media && media.data ? [media] : []);
                    if (mediaList.length > 0) {
                        for (let i = 0; i < mediaList.length; i++) {
                            const mItem = mediaList[i];
                            if (!mItem || !mItem.data) continue;
                            const buffer = Buffer.from(mItem.data, 'base64');
                            let mediaMessage = {};
                            let mime = mItem.mimetype ? mItem.mimetype.toLowerCase() : '';
                            const name = mItem.filename ? mItem.filename.toLowerCase() : '';
                            
                            if (!mime || mime === 'application/octet-stream') {
                                const ext = name.split('.').pop();
                                const mimeMap = {
                                    'png': 'image/png', 'jpg': 'image/jpeg', 'jpeg': 'image/jpeg', 'gif': 'image/gif', 'webp': 'image/webp',
                                    'mp4': 'video/mp4', 'mov': 'video/quicktime', 'pdf': 'application/pdf', 'csv': 'text/csv',
                                    'doc': 'application/msword', 'docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document', 'txt': 'text/plain'
                                };
                                mime = mimeMap[ext] || mime;
                            }

                            const isImage = mime.startsWith('image/') || name.match(/\.(jpg|jpeg|png|gif|webp)$/);
                            const isVideo = mime.startsWith('video/') || name.match(/\.(mp4|mov|avi|webm|mkv)$/);
                            const isAudio = mime.startsWith('audio/') || name.match(/\.(mp3|wav|ogg|m4a|aac|opus)$/);

                            // Send caption only with the last file or text if provided
                            const currentCaption = (i === mediaList.length - 1) ? text : '';

                            if (isImage) {
                                mediaMessage = { image: buffer, caption: currentCaption, mimetype: mime || 'image/jpeg' };
                            } else if (isVideo) {
                                mediaMessage = { video: buffer, caption: currentCaption, mimetype: mime || 'video/mp4' };
                            } else if (isAudio) {
                                mediaMessage = { audio: buffer, mimetype: 'audio/ogg; codecs=opus', ptt: true };
                            } else {
                                mediaMessage = { document: buffer, caption: currentCaption, mimetype: mime || 'application/octet-stream', fileName: mItem.filename || 'document' };
                            }
                            await whatsappClient.sendMessage(formattedNumber, mediaMessage);
                            if (i < mediaList.length - 1) await new Promise(r => setTimeout(r, 500));
                        }
                    } else {
                        await whatsappClient.sendMessage(formattedNumber, { text: text });
                    }
                    console.log(`✅ Message sent to ${num}`);
                    socket.emit("bulk_progress_update", { status: "sent", number: num });
                    successCount++;
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

        socket.emit("bulk_progress_completed", { success: true });

        if (typeof callback === "function") {
            callback({ success: true });
        }
    });

    // ── Progressive Web Scraper via Socket.IO ──────────────────────────────
    // The frontend emits "start_scrape" → backend streams results back via
    // "scrape_lead", "scrape_phone_update", and "scrape_complete" events.
    socket.on("start_scrape", async ({ query, location }) => {
      if (!query) {
        socket.emit("scrape_error", { message: "Search query is required." });
        return;
      }
      console.log(`🌐 [Socket] Scrape request: "${query}" in "${location || "Any"}"`);

      try {
        await scrapeGoogleMapsLeads(query, location || "", {
          onLead: (lead) => {
            socket.emit("scrape_lead", lead);
          },
          onPhoneUpdate: (id, phone) => {
            socket.emit("scrape_phone_update", { id, phone });
          },
          onComplete: (leads) => {
            socket.emit("scrape_complete", { count: leads.length });
          }
        });
      } catch (err) {
        console.error("❌ [Socket Scrape Error]:", err.message);
        socket.emit("scrape_error", { message: err.message || "Scraping failed. Try a different search." });
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
 
                    const rawMedia = scheduleDoc.media;
                    const mediaList = Array.isArray(rawMedia) ? rawMedia : (rawMedia && (rawMedia.data || rawMedia.length) ? (Array.isArray(rawMedia) ? rawMedia : [rawMedia]) : []);
                    if (mediaList.length > 0) {
                        for (let i = 0; i < mediaList.length; i++) {
                            const mItem = mediaList[i];
                            if (!mItem || !mItem.data) continue;
                            const buffer = Buffer.from(mItem.data, 'base64');
                            let mediaMessage = {};
                            let mime = mItem.mimetype ? mItem.mimetype.toLowerCase() : '';
                            const name = mItem.filename ? mItem.filename.toLowerCase() : '';
                            
                            if (!mime || mime === 'application/octet-stream') {
                                const ext = name.split('.').pop();
                                const mimeMap = {
                                    'png': 'image/png', 'jpg': 'image/jpeg', 'jpeg': 'image/jpeg', 'gif': 'image/gif', 'webp': 'image/webp',
                                    'mp4': 'video/mp4', 'mov': 'video/quicktime', 'pdf': 'application/pdf', 'csv': 'text/csv',
                                    'doc': 'application/msword', 'docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document', 'txt': 'text/plain'
                                };
                                mime = mimeMap[ext] || mime;
                            }

                            const isImage = mime.startsWith('image/') || name.match(/\.(jpg|jpeg|png|gif|webp)$/);
                            const isVideo = mime.startsWith('video/') || name.match(/\.(mp4|mov|avi|webm|mkv)$/);
                            const isAudio = mime.startsWith('audio/') || name.match(/\.(mp3|wav|ogg|m4a|aac|opus)$/);

                            const currentCaption = (i === mediaList.length - 1) ? scheduleDoc.message : '';

                            if (isImage) {
                                mediaMessage = { image: buffer, caption: currentCaption, mimetype: mime || 'image/jpeg' };
                            } else if (isVideo) {
                                mediaMessage = { video: buffer, caption: currentCaption, mimetype: mime || 'video/mp4' };
                            } else if (isAudio) {
                                mediaMessage = { audio: buffer, mimetype: 'audio/ogg; codecs=opus', ptt: true };
                            } else {
                                mediaMessage = { document: buffer, caption: currentCaption, mimetype: mime || 'application/octet-stream', fileName: mItem.filename || 'document' };
                            }
                            await whatsappClient.sendMessage(formattedNumber, mediaMessage);
                            if (i < mediaList.length - 1) await new Promise(r => setTimeout(r, 500));
                        }
                    } else {
                        await whatsappClient.sendMessage(formattedNumber, { text: scheduleDoc.message });
                    }
                    io.emit("bulk_progress_update", { status: "sent", number: num });
                    successCount++;
                    await new Promise(resolve => setTimeout(resolve, 2000));
                } catch(e) {
                    console.error('Error sending scheduled to', num, e);
                    let reason = e.message || "Unable to send message.";
                    if (reason.includes("reading 'id'") || reason.includes("undefined") || reason.includes("Cannot read properties")) {
                        reason = "Number is not registered on WhatsApp.";
                    }
                    io.emit("bulk_progress_update", { status: "failed", number: num, reason: reason });
                    failedCount++;
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
  credits: { type: Number, default: 0 },
  totalSent: { type: Number, default: 0 },
  lastDailyRewardDate: { type: String, default: "" }
});

const messageLogSchema = new mongoose.Schema({
  date: { type: String, required: true, unique: true }, // Format: YYYY-MM-DD
  count: { type: Number, default: 0 },
  successCount: { type: Number, default: 0 },
  failedCount: { type: Number, default: 0 }
});

// Avoid OverwriteModelError if it's already defined
const User = mongoose.models.User || mongoose.model('User', userSchema);
const MessageLog = mongoose.models.MessageLog || mongoose.model('MessageLog', messageLogSchema);

app.post('/api/register', async (req, res) => {
  try {
    const { name, email, password, phone, location } = req.body;
    const existingUser = await User.findOne({ email });
    if (existingUser) return res.status(400).json({ message: "User already exists" });
    
    // Explicitly set 30 credits and today's date so they don't get another 30 on login
    const d = new Date();
    const offset = d.getTimezoneOffset() * 60000;
    const today = new Date(d.getTime() - offset).toISOString().split('T')[0];

    const newUser = new User({ 
      name, email, password, phone, location,
      credits: 30,
      lastDailyRewardDate: today
    });
    
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
    
    // Daily Credit Reward Logic
    const d = new Date();
    const offset = d.getTimezoneOffset() * 60000;
    const today = new Date(d.getTime() - offset).toISOString().split('T')[0];

    if (user.lastDailyRewardDate !== today) {
      user.credits = (user.credits || 0) + 30;
      user.lastDailyRewardDate = today;
      await user.save();
    }

    res.status(200).json({ 
      message: "Login successful!", 
      name: user.name, 
      email: user.email, 
      phone: user.phone || "", 
      location: user.location || "", 
      credits: user.credits,
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

app.post('/api/user/credits', async (req, res) => {
  try {
    const { email } = req.body;
    let user = await User.findOne({ email });
    if (!user) return res.status(404).json({ message: "User not found" });

    const d = new Date();
    const offset = d.getTimezoneOffset() * 60000;
    const today = new Date(d.getTime() - offset).toISOString().split('T')[0];

    let rewarded = false;
    if (user.lastDailyRewardDate !== today) {
      user.credits = (user.credits || 0) + 30;
      user.lastDailyRewardDate = today;
      await user.save();
      rewarded = true;
    }

    res.status(200).json({ credits: user.credits, totalSent: user.totalSent || 0, rewarded, lastDailyRewardDate: user.lastDailyRewardDate });
  } catch (error) {
    res.status(500).json({ message: "Error fetching credits", error });
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

app.post('/api/user/use-credit', async (req, res) => {
  try {
    const { email, amount } = req.body;
    let user = await User.findOne({ email });
    if (user) {
      user.credits = Math.max(0, (user.credits || 0) - (amount || 1));
      await user.save();
      res.status(200).json({ credits: user.credits });
    } else {
      res.status(404).json({ message: "User not found" });
    }
  } catch (error) {
    res.status(500).json({ message: "Error using credit", error });
  }
});

app.post('/api/contact/whatsapp', async (req, res) => {
  try {
    const { name, email, subject, message, text } = req.body;

    const validationErrors = [];
    if (!name || !String(name).trim()) validationErrors.push('Please enter your full name.');
    if (!email || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(String(email))) validationErrors.push('Please enter a valid email address.');
    if (!subject || !String(subject).trim()) validationErrors.push('Please enter a subject.');
    if (!message || !String(message).trim()) validationErrors.push('Please enter your message.');

    if (validationErrors.length > 0) {
      return res.status(400).json({ message: validationErrors[0] });
    }

    const activeClient = Object.values(userClients).find(uc => uc.isWhatsAppAuthenticated)?.whatsappClient;
    if (!activeClient) {
      return res.status(503).json({ message: 'WhatsApp is not ready yet. Please try again shortly.' });
    }

    const destination = String(process.env.WHATSAPP_CONTACT_NUMBER || '919486042369').replace(/\D/g, '');
    if (!destination) {
      return res.status(500).json({ message: 'WhatsApp destination is not configured.' });
    }

    const formattedNumber = destination.startsWith('91') ? `${destination}@s.whatsapp.net` : `91${destination}@s.whatsapp.net`;
    const payload = { text: text || `New contact form submission\nName: ${name}\nEmail: ${email}\nSubject: ${subject}\nMessage: ${message}` };

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

    const destination = '919524648478';
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

// Scraper API Route
app.post("/api/scrape-leads", async (req, res) => {
  try {
    const { query, location } = req.body;
    if (!query) {
      return res.status(400).json({ success: false, message: "Query string is required" });
    }
    const leads = await scrapeGoogleMapsLeads(query, location || "");
    return res.json({ success: true, count: leads.length, leads });
  } catch (err) {
    console.error("API Error:", err);
    return res.status(500).json({ success: false, message: "Scraping failed" });
  }
});


server.listen(PORT, () => console.log(`Server running on port ${PORT}`));