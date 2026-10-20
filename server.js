const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const Redis = require('ioredis');
const { createAdapter } = require('@socket.io/redis-adapter');

const app = express();
app.use(cors());
const server = http.createServer(app);

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

io.on('connection', (socket) => {
  console.log('User connected:', socket.id);

  // 🔥 FAST BUNDLE MATCHMAKING (Across 2 Render Servers) 🔥
  socket.on('findStranger', async (userProfile) => {
    try {
      // 1. Pehle is user ki koi purani duplicate entry queue me ho toh hatao
      await redisClient.lrem('appie_waiting_queue', 0, socket.id);

      // 2. User ka profile bundle Redis me 2 minute ke liye cache karo
      const myMeta = (typeof userProfile === 'object' && userProfile !== null) ? userProfile : {};
      myMeta.socketId = socket.id;
      await redisClient.set(`meta:${socket.id}`, JSON.stringify(myMeta), 'EX', 120);

      let matchedPartnerId = null;
      let partnerMeta = {};

      // 3. Queue se tab tak pop karo jab tak ek LIVE aur ALAG user na mil jaye
      while (true) {
        const candidateId = await redisClient.lpop('appie_waiting_queue');
        if (!candidateId) break; // Queue khali hai

        if (candidateId === socket.id) continue; // Khud se match mat karo

        // Check karo ki candidate abhi bhi kisi Render server par connected hai ya nahi
        const activeSockets = await io.in(candidateId).allSockets();
        if (!activeSockets || activeSockets.size === 0) {
          continue; // Dead/Ghost socket hai, skip karo!
        }

        // Candidate ka profile bundle nikalo
        const rawMeta = await redisClient.get(`meta:${candidateId}`);
        if (rawMeta) {
          try { partnerMeta = JSON.parse(rawMeta); } catch (e) {}
        }

        // Agar dono ki Firebase UID same hai, toh wapas queue me daal kar chhod do
        if (myMeta.uid && partnerMeta.uid && myMeta.uid === partnerMeta.uid) {
          await redisClient.rpush('appie_waiting_queue', candidateId);
          break;
        }

        matchedPartnerId = candidateId;
        break;
      }

      if (matchedPartnerId) {
        const roomId = `room_${matchedPartnerId}_${socket.id}`;

        // Server khud dono sockets ko room me join kara dega (0ms delay)
        io.in(matchedPartnerId).socketsJoin(roomId);
        socket.join(roomId);

        // Dono ko matchFound ke sath samne wale ka pura Profile Bundle bhej do!
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
        // Koi nahi mila, queue me lagao
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
    if (data && data.roomId) {
      socket.to(data.roomId).emit('webrtcOffer', data.offer || data);
    }
  });

  socket.on('webrtcAnswer', (data) => {
    if (data && data.roomId) {
      socket.to(data.roomId).emit('webrtcAnswer', data.answer || data);
    }
  });

  socket.on('iceCandidate', (data) => {
    if (data && data.roomId) {
      socket.to(data.roomId).emit('iceCandidate', data.candidate || data);
    }
  });

  socket.on('leaveChat', async (roomId) => {
    await redisClient.lrem('appie_waiting_queue', 0, socket.id);
    if (roomId) {
      socket.to(roomId).emit('strangerDisconnected');
      socket.leave(roomId);
    }
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
  console.log(`🚀 Appie Redis Master Server running on port ${PORT}`);
});
