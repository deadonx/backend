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
// 1. CONFIG (SUPABASE, REDIS, SERVER LIMIT)
// ==========================================
const SUPABASE_URL = process.env.SUPABASE_URL || "https://pxzofsaallaeezqyofin.supabase.co";
const SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InB4em9mc2FhbGxhZWV6cXlvZmluIiwicm9sZSI6ImFub24iLCJpYXQiOjE3OTI2Njk0MDYsImV4cCI6MjEwODI0NTQwNn0.yiPaUkq6GRNK8rB-wYqj9hlZTr7Mp4_6fPCk-Vv-Lgw";
const REDIS_URL = process.env.REDIS_URL || "rediss://default:gQAAAAAAAcMpAAIgcDI3M2UyMDQwNGJhN2M0MWNhYmVlNGQzYTlmY2EyMzlmMQ@credible-sunbird-115497.upstash.io:6379";

const MAX_USERS = parseInt(process.env.MAX_USERS || "500", 10);
const QUEUE_KEY = 'appie:queue';

const redisOptions = {
  tls: { rejectUnauthorized: false },
  maxRetriesPerRequest: null,
  retryStrategy: (times) => Math.min(times * 100, 3000)
};

const pubClient = new Redis(REDIS_URL, redisOptions);
const subClient = pubClient.duplicate();
const redisClient = new Redis(REDIS_URL, redisOptions);

pubClient.on('connect', () => console.log('🔥 Redis Connected'));
pubClient.on('error', (err) => console.error('Redis Pub Error:', err.message));
subClient.on('error', (err) => console.error('Redis Sub Error:', err.message));
redisClient.on('error', (err) => console.error('Redis Client Error:', err.message));

const io = new Server(server, {
  cors: { origin: "*", methods: ["GET", "POST"] },
  transports: ["websocket", "polling"],
  pingTimeout: 10000,
  pingInterval: 5000
});

io.adapter(createAdapter(pubClient, subClient));

// ==========================================
// 2. ATOMIC LUA MATCHMAKING (1ms, ZERO LOCK)
// ==========================================
const MATCH_LUA = `
local queueKey = KEYS[1]
local myId = ARGV[1]
local myUid = ARGV[2]
local myMeta = ARGV[3]

redis.call('SET', 'online:' .. myId, '1', 'EX', 1800)
redis.call('SET', 'uid:' .. myId, myUid, 'EX', 1800)
redis.call('SET', 'meta:' .. myId, myMeta, 'EX', 600)
redis.call('LREM', queueKey, 0, myId)

local skipped = {}
local matchedId = nil
local matchedMeta = nil

while true do
  local candId = redis.call('LPOP', queueKey)
  if not candId then
    break
  end
  if candId ~= myId then
    if redis.call('EXISTS', 'online:' .. candId) == 1 then
      local candUid = redis.call('GET', 'uid:' .. candId) or ''
      local candMeta = redis.call('GET', 'meta:' .. candId)
      if candMeta then
        if myUid ~= '' and candUid ~= '' and myUid == candUid then
          table.insert(skipped, candId)
        else
          matchedId = candId
          matchedMeta = candMeta
          break
        end
      end
    end
  end
end

for i = 1, #skipped do
  redis.call('RPUSH', queueKey, skipped[i])
end

if matchedId then
  redis.call('SET', 'partner:' .. myId, matchedId, 'EX', 3600)
  redis.call('SET', 'partner:' .. matchedId, myId, 'EX', 3600)
  return {matchedId, matchedMeta}
else
  redis.call('RPUSH', queueKey, myId)
  redis.call('EXPIRE', queueKey, 300)
  return nil
end
`;

// ==========================================
// 3. REST APIs
// ==========================================
app.get('/', (req, res) => {
  res.json({
    status: 'Appie Live',
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
    if (Array.isArray(data) && data.length > 0) res.json(data[0]);
    else res.status(404).json({ error: "Profile not found" });
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
// 4. PARTNER CLEANUP HELPER
// ==========================================
async function leavePartner(socketId) {
  try {
    await redisClient.lrem(QUEUE_KEY, 0, socketId);
    const partnerId = await redisClient.get(`partner:${socketId}`);
    if (partnerId) {
      await redisClient.del(`partner:${socketId}`, `partner:${partnerId}`);
      io.to(partnerId).emit('strangerDisconnected', { from: socketId });
    }
  } catch (err) {
    console.error('Cleanup error:', err.message);
  }
}

// ==========================================
// 5. SOCKET.IO MATCHMAKING & DIRECT SIGNALING
// ==========================================
io.on('connection', (socket) => {
  if (io.engine.clientsCount > MAX_USERS) {
    socket.emit('serverFull');
    socket.disconnect(true);
    return;
  }

  console.log(`✅ Connected: ${socket.id} (${io.engine.clientsCount}/${MAX_USERS})`);
  redisClient.set(`online:${socket.id}`, '1', 'EX', 1800);

  socket.on('findStranger', async (profile) => {
    try {
      await leavePartner(socket.id);

      const myProfile = (profile && typeof profile === 'object') ? profile : {};
      const myUid = String(myProfile.uid || '');

      const result = await redisClient.eval(
        MATCH_LUA,
        1,
        QUEUE_KEY,
        socket.id,
        myUid,
        JSON.stringify(myProfile)
      );

      if (result && result.length === 2) {
        const partnerId = result[0];
        let partnerProfile = {};
        try { partnerProfile = JSON.parse(result[1]); } catch (e) {}

        // Pehle wait karne wala = Caller
        io.to(partnerId).emit('matchFound', {
          partnerId: socket.id,
          role: 'caller',
          profile: myProfile
        });

        // Abhi aaya hua = Callee (PeerConnection banakar calleeReady bhejega)
        io.to(socket.id).emit('matchFound', {
          partnerId: partnerId,
          role: 'callee',
          profile: partnerProfile
        });

        console.log(`⚡ Match: Caller(${partnerId}) <-> Callee(${socket.id})`);
      } else {
        console.log(`⏳ Waiting: ${socket.id}`);
      }
    } catch (err) {
      console.error('findStranger error:', err.message);
    }
  });

  // Proven calleeReady -> webrtcOffer -> webrtcAnswer -> iceCandidate (2-server safe)
  socket.on('calleeReady', (data) => {
    if (data && data.to) {
      console.log(`📡 calleeReady: ${socket.id} -> ${data.to}`);
      io.to(data.to).emit('calleeReady', { from: socket.id });
    }
  });

  socket.on('webrtcOffer', (data) => {
    if (data && data.to && data.sdp) {
      console.log(`📡 Offer: ${socket.id} -> ${data.to}`);
      io.to(data.to).emit('webrtcOffer', { from: socket.id, sdp: data.sdp });
    }
  });

  socket.on('webrtcAnswer', (data) => {
    if (data && data.to && data.sdp) {
      console.log(`📡 Answer: ${socket.id} -> ${data.to}`);
      io.to(data.to).emit('webrtcAnswer', { from: socket.id, sdp: data.sdp });
    }
  });

  socket.on('iceCandidate', (data) => {
    if (data && data.to && data.candidate) {
      io.to(data.to).emit('iceCandidate', { from: socket.id, candidate: data.candidate });
    }
  });

  socket.on('leaveChat', async () => {
    await leavePartner(socket.id);
  });

  socket.on('disconnect', async () => {
    console.log(`❌ Disconnected: ${socket.id}`);
    await redisClient.del(`online:${socket.id}`, `uid:${socket.id}`, `meta:${socket.id}`);
    await leavePartner(socket.id);
  });
});

const PORT = process.env.PORT || 10000;
server.listen(PORT, () => {
  console.log(`🚀 Appie Server running on port ${PORT} (Max: ${MAX_USERS})`);
});
