const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const Redis = require('ioredis');
const { createAdapter } = require('@socket.io/redis-adapter');

const app = express();
app.use(cors());
app.use(express.json());

const server = http.createServer(app);

// ==========================================
// 1. CONFIG (SUPABASE, REDIS & SERVER LIMIT)
// ==========================================
const SUPABASE_URL = process.env.SUPABASE_URL || "https://pxzofsaallaeezqyofin.supabase.co";
const SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InB4em9mc2FhbGxhZWV6cXlvZmluIiwicm9sZSI6ImFub24iLCJpYXQiOjE3OTI2Njk0MDYsImV4cCI6MjEwODI0NTQwNn0.yiPaUkq6GRNK8rB-wYqj9hlZTr7Mp4_6fPCk-Vv-Lgw";
const REDIS_URL = process.env.REDIS_URL || "rediss://default:gQAAAAAAAcMpAAIgcDI3M2UyMDQwNGJhN2M0MWNhYmVlNGQzYTlmY2EyMzlmMQ@credible-sunbird-115497.upstash.io:6379";

// Ek server par max live users (cross hote hi user Server 2 par redirect hoga)
const MAX_USERS = parseInt(process.env.MAX_USERS || "500", 10);

const redisOptions = {
  tls: { rejectUnauthorized: false },
  maxRetriesPerRequest: null,
  retryStrategy: (times) => Math.min(times * 100, 3000)
};

const pubClient = new Redis(REDIS_URL, redisOptions);
const subClient = pubClient.duplicate();
const redisClient = new Redis(REDIS_URL, redisOptions);

pubClient.on('connect', () => console.log('🔥 Redis Pub Connected'));
pubClient.on('error', (err) => console.error('Redis Pub Error:', err.message));
subClient.on('error', (err) => console.error('Redis Sub Error:', err.message));
redisClient.on('error', (err) => console.error('Redis Client Error:', err.message));

const io = new Server(server, {
  cors: { origin: "*", methods: ["GET", "POST"] },
  transports: ["websocket", "polling"],
  pingTimeout: 10000,
  pingInterval: 5000
});

// Dono Render servers ko aapas mein connect karne wala Redis Adapter
io.adapter(createAdapter(pubClient, subClient));

// ==========================================
// 2. REST APIs (HEALTH + SUPABASE)
// ==========================================
app.get('/', (req, res) => {
  res.json({
    status: 'Appie Signaling Server Live',
    activeUsers: io.engine.clientsCount,
    maxUsers: MAX_USERS
  });
});

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
    if (Array.isArray(data) && data.length > 0) {
      res.json(data[0]);
    } else {
      res.status(404).json({ error: "Profile not found" });
    }
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/updateExp', async (req, res) => {
  const { uid, newExp } = req.body;
  if (!uid || newExp === undefined) return res.status(400).json({ error: "Data missing" });

  try {
    await fetch(`${SUPABASE_URL}/rest/v1/public_profiles?uid=eq.${uid}`, {
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
// 3. REDIS HELPERS (LOCK + CLEANUP)
// ==========================================
const QUEUE_KEY = 'appie:waiting_queue';
const LOCK_KEY = 'appie:match_lock';

async function acquireLock() {
  for (let i = 0; i < 25; i++) {
    const acquired = await redisClient.set(LOCK_KEY, '1', 'PX', 2500, 'NX');
    if (acquired) return true;
    await new Promise((resolve) => setTimeout(resolve, 40));
  }
  return false;
}

async function releaseLock() {
  try {
    await redisClient.del(LOCK_KEY);
  } catch (e) {}
}

async function cleanupUserState(socketId, notifyPartner = true) {
  try {
    await redisClient.lrem(QUEUE_KEY, 0, socketId);
    const partnerId = await redisClient.get(`partner:${socketId}`);

    if (partnerId) {
      await redisClient.del(`partner:${socketId}`);
      await redisClient.del(`partner:${partnerId}`);
      if (notifyPartner) {
        io.to(partnerId).emit('strangerDisconnected', { from: socketId });
      }
    }
  } catch (err) {
    console.error('Cleanup error:', err.message);
  }
}

// ==========================================
// 4. SOCKET.IO MATCHMAKING & SIGNALING
// ==========================================
io.on('connection', (socket) => {
  // Server full hone par user ko automatically Server 2 par bhej dein
  if (io.engine.clientsCount > MAX_USERS) {
    console.log(`⚠️ Server full (${io.engine.clientsCount}/${MAX_USERS}), redirecting ${socket.id}`);
    socket.emit('serverFull');
    socket.disconnect(true);
    return;
  }

  console.log(`✅ Connected: ${socket.id} (${io.engine.clientsCount}/${MAX_USERS})`);
  redisClient.set(`online:${socket.id}`, '1', 'EX', 1800);

  // Step 1: User starts searching
  socket.on('findStranger', async (userProfile) => {
    const locked = await acquireLock();
    try {
      await cleanupUserState(socket.id, true);
      await redisClient.set(`online:${socket.id}`, '1', 'EX', 1800);

      const myProfile = (userProfile && typeof userProfile === 'object') ? userProfile : {};
      myProfile.socketId = socket.id;
      await redisClient.set(`meta:${socket.id}`, JSON.stringify(myProfile), 'EX', 300);

      let partnerId = null;
      let partnerProfile = null;
      const skippedIds = [];

      while (true) {
        const candidateId = await redisClient.lpop(QUEUE_KEY);
        if (!candidateId) break;
        if (candidateId === socket.id) continue;

        // Fast cross-server online check (2 servers ke beech 100% reliable)
        const isOnline = await redisClient.exists(`online:${candidateId}`);
        if (!isOnline) {
          await redisClient.del(`meta:${candidateId}`);
          continue;
        }

        const rawMeta = await redisClient.get(`meta:${candidateId}`);
        if (!rawMeta) continue;

        let candidateProfile = {};
        try {
          candidateProfile = JSON.parse(rawMeta);
        } catch (e) {
          continue;
        }

        // Same Google UID wale ko khud se match mat karo
        if (myProfile.uid && candidateProfile.uid && myProfile.uid === candidateProfile.uid) {
          skippedIds.push(candidateId);
          continue;
        }

        partnerId = candidateId;
        partnerProfile = candidateProfile;
        break;
      }

      // Same-UID skipped sockets wapas queue mein daal dein
      for (const id of skippedIds) {
        await redisClient.rpush(QUEUE_KEY, id);
      }

      if (partnerId && partnerProfile) {
        await redisClient.set(`partner:${partnerId}`, socket.id, 'EX', 3600);
        await redisClient.set(`partner:${socket.id}`, partnerId, 'EX', 3600);

        // Pehle se wait karne wala user -> CALLER
        io.to(partnerId).emit('matchFound', {
          partnerSocketId: socket.id,
          role: 'caller',
          partnerProfile: myProfile
        });

        // Abhi aaya hua user -> CALLEE
        io.to(socket.id).emit('matchFound', {
          partnerSocketId: partnerId,
          role: 'callee',
          partnerProfile: partnerProfile
        });

        console.log(`⚡ Match: Caller(${partnerId}) <-> Callee(${socket.id})`);
      } else {
        await redisClient.rpush(QUEUE_KEY, socket.id);
        await redisClient.expire(QUEUE_KEY, 300);
        socket.emit('waiting');
        console.log(`⏳ Waiting in queue: ${socket.id}`);
      }
    } catch (err) {
      console.error('Matchmaking error:', err.message);
    } finally {
      if (locked) await releaseLock();
    }
  });

  // Step 2: Direct Socket-to-Socket Signaling (Cross-server safe)
  socket.on('calleeReady', (data) => {
    if (data && data.to) {
      console.log(`📡 calleeReady: ${socket.id} -> ${data.to}`);
      io.to(data.to).emit('calleeReady', { from: socket.id });
    }
  });

  socket.on('webrtcOffer', (data) => {
    if (data && data.to && data.sdp) {
      console.log(`📡 Offer: ${socket.id} -> ${data.to}`);
      io.to(data.to).emit('webrtcOffer', {
        from: socket.id,
        sdp: data.sdp
      });
    }
  });

  socket.on('webrtcAnswer', (data) => {
    if (data && data.to && data.sdp) {
      console.log(`📡 Answer: ${socket.id} -> ${data.to}`);
      io.to(data.to).emit('webrtcAnswer', {
        from: socket.id,
        sdp: data.sdp
      });
    }
  });

  socket.on('iceCandidate', (data) => {
    if (data && data.to && data.candidate) {
      io.to(data.to).emit('iceCandidate', {
        from: socket.id,
        candidate: data.candidate
      });
    }
  });

  // User NEXT dabaye ya call chhode
  socket.on('leaveChat', async () => {
    await cleanupUserState(socket.id, true);
  });

  // User app band kare ya net band ho
  socket.on('disconnect', async () => {
    console.log(`❌ Disconnected: ${socket.id}`);
    await redisClient.del(`online:${socket.id}`);
    await cleanupUserState(socket.id, true);
    await redisClient.del(`meta:${socket.id}`);
  });
});

const PORT = process.env.PORT || 10000;
server.listen(PORT, () => {
  console.log(`🚀 Appie Server running on port ${PORT} (Max Users: ${MAX_USERS})`);
});
