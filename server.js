const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const Redis = require('ioredis');
const { createAdapter } = require('@socket.io/redis-adapter');

const app = express();
app.use(cors());
const server = http.createServer(app);

// ==========================================
// 🔥 SUPABASE CONFIG (Yahan apni keys daalein) 🔥
// ==========================================
const SUPABASE_URL = "https://pxzofsaallaeezqyofin.supabase.co"; // Apna Project URL yahan daalein
const SUPABASE_ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InB4em9mc2FhbGxhZWV6cXlvZmluIiwicm9sZSI6ImFub24iLCJpYXQiOjE3OTI2Njk0MDYsImV4cCI6MjEwODI0NTQwNn0.yiPaUkq6GRNK8rB-wYqj9hlZTr7Mp4_6fPCk-Vv-Lgw"; // Apni Publishable key yahan daalein

const REDIS_URL = process.env.REDIS_URL || "rediss://default:gQAAAAAAAcMpAAIgcDI3M2UyMDQwNGJhN2M0MWNhYmVlNGQzYTlmY2EyMzlmMQ@credible-sunbird-115497.upstash.io:6379";

const redisOptions = {
  tls: { rejectUnauthorized: false },
  retryStrategy: (times) => Math.min(times * 50, 2000)
};

const pubClient = new Redis(REDIS_URL, redisOptions);
const subClient = pubClient.duplicate();
const redisClient = new Redis(REDIS_URL, redisOptions);

const io = new Server(server, {
  cors: { origin: "*", methods: ["GET", "POST"] },
  pingTimeout: 10000,
  pingInterval: 5000
});

io.adapter(createAdapter(pubClient, subClient));

pubClient.on('connect', () => console.log('🔥 Redis Pub/Sub Connected!'));

// ==========================================
// 🔥 REST APIs (Android App Data ke liye) 🔥
// ==========================================

// 1. App open hone par Profile Data lane ke liye API
app.get('/getProfile', async (req, res) => {
  const uid = req.query.uid;
  if (!uid) return res.status(400).json({ error: "UID missing" });

  try {
    const response = await fetch(`${SUPABASE_URL}/rest/v1/public_profiles?uid=eq.${uid}&select=*`, {
      headers: {
        'apikey': SUPABASE_ANON_KEY,
        'Authorization': `Bearer ${SUPABASE_ANON_KEY}`
      }
    });
    const data = await response.json();
    
    if (data && data.length > 0) {
      res.json(data[0]); // Data mil gaya
    } else {
      res.json({ error: "Profile not found" });
    }
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// 2. Call khatam hone ke baad EXP update karne ke liye API
app.post('/updateExp', express.json(), async (req, res) => {
  const { uid, newExp } = req.body;
  if (!uid || newExp === undefined) return res.status(400).json({ error: "Data missing" });

  try {
    const response = await fetch(`${SUPABASE_URL}/rest/v1/public_profiles?uid=eq.${uid}`, {
      method: 'PATCH',
      headers: {
        'Content-Type': 'application/json',
        'apikey': SUPABASE_ANON_KEY,
        'Authorization': `Bearer ${SUPABASE_ANON_KEY}`,
        'Prefer': 'return=minimal'
      },
      body: JSON.stringify({ exp: newExp })
    });
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});


// ==========================================
// 🔥 SOCKET.IO LIVE MATCHMAKING LOGIC 🔥
// ==========================================

io.on('connection', (socket) => {
  console.log('User connected:', socket.id);

  socket.on('findStranger', async (userProfile) => {
    try {
      await redisClient.lrem('appie_waiting_queue', 0, socket.id);

      const myMeta = (typeof userProfile === 'object' && userProfile !== null) ? userProfile : {};
      myMeta.socketId = socket.id;
      await redisClient.set(`meta:${socket.id}`, JSON.stringify(myMeta), 'EX', 120);

      let matchedPartnerId = null;
      let partnerMeta = {};

      while (true) {
        const candidateId = await redisClient.lpop('appie_waiting_queue');
        if (!candidateId) break; 

        if (candidateId === socket.id) continue; 

        const rawMeta = await redisClient.get(`meta:${candidateId}`);
        if (!rawMeta) continue;

        try { partnerMeta = JSON.parse(rawMeta); } catch (e) {}

        if (myMeta.uid && partnerMeta.uid && myMeta.uid === partnerMeta.uid) {
          await redisClient.rpush('appie_waiting_queue', candidateId);
          break;
        }

        matchedPartnerId = candidateId;
        break; 
      }

      if (matchedPartnerId) {
        const roomId = `room_${matchedPartnerId}_${socket.id}`;

        io.in(matchedPartnerId).socketsJoin(roomId);
        socket.join(roomId);

        io.to(matchedPartnerId).emit('matchFound', {
          roomId: roomId,
          role: 'caller',
          partnerProfile: myMeta
        });

        io.to(socket.id).emit('matchFound', {
          roomId: roomId,
          role: 'callee',
          partnerProfile: partnerMeta
        });

        console.log(`⚡ Instant Match! Room: ${roomId}`);
      } else {
        await redisClient.rpush('appie_waiting_queue', socket.id);
        await redisClient.expire('appie_waiting_queue', 120);
        console.log(`User ${socket.id} waiting in Redis queue...`);
      }
    } catch (err) {
      console.error('Matchmaking Error:', err);
    }
  });

  socket.on('joinRoom', (roomId) => {
    if (roomId) socket.join(roomId);
  });

  socket.on('calleeReady', (roomId) => {
    if (roomId) socket.to(roomId).emit('calleeReady');
  });

  socket.on('webrtcOffer', (data) => {
    if (data && data.roomId) socket.to(data.roomId).emit('webrtcOffer', data.offer || data);
  });

  socket.on('webrtcAnswer', (data) => {
    if (data && data.roomId) socket.to(data.roomId).emit('webrtcAnswer', data.answer || data);
  });

  socket.on('iceCandidate', (data) => {
    if (data && data.roomId) socket.to(data.roomId).emit('iceCandidate', data.candidate || data);
  });

  socket.on('leaveChat', async () => {
    await redisClient.lrem('appie_waiting_queue', 0, socket.id);
    socket.rooms.forEach((roomId) => {
      if (roomId !== socket.id) {
        socket.to(roomId).emit('strangerDisconnected');
        socket.leave(roomId);
      }
    });
  });

  socket.on('disconnect', async () => {
    console.log('User disconnected:', socket.id);
    await redisClient.lrem('appie_waiting_queue', 0, socket.id);
    await redisClient.del(`meta:${socket.id}`);
    
    socket.rooms.forEach((roomId) => {
      if (roomId !== socket.id) {
        socket.to(roomId).emit('strangerDisconnected');
      }
    });
  });
});

const PORT = process.env.PORT || 10000;
server.listen(PORT, () => {
  console.log(`🚀 Appie Server running on port ${PORT}`);
});
