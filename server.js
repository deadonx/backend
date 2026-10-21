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

  // 🔥 INSTANT MATCHMAKING LOGIC 🔥
  socket.on('findStranger', async (userProfile) => {
    try {
      // 1. Purani aadhi-adhuri entry queue se hatao
      await redisClient.lrem('appie_waiting_queue', 0, socket.id);

      // 2. User ka profile data Redis me cache karo (Sirf 120 seconds ke liye)
      const myMeta = (typeof userProfile === 'object' && userProfile !== null) ? userProfile : {};
      myMeta.socketId = socket.id;
      await redisClient.set(`meta:${socket.id}`, JSON.stringify(myMeta), 'EX', 120);

      let matchedPartnerId = null;
      let partnerMeta = {};

      // 3. Queue search loop
      while (true) {
        const candidateId = await redisClient.lpop('appie_waiting_queue');
        if (!candidateId) break; // Queue khali hai

        if (candidateId === socket.id) continue; // Khud se match mat karo

        // 🔥 NAYA FIX: Active connection check karne ka fast tarika
        const rawMeta = await redisClient.get(`meta:${candidateId}`);
        if (!rawMeta) {
            // Agar meta delete ho gaya (user chhod ke chala gaya), toh is ghost ko ignore karo
            continue;
        }

        try { partnerMeta = JSON.parse(rawMeta); } catch (e) {}

        // Dono ki Firebase UID same ho, toh match mat karo (Same account on 2 phones)
        if (myMeta.uid && partnerMeta.uid && myMeta.uid === partnerMeta.uid) {
          await redisClient.rpush('appie_waiting_queue', candidateId);
          break;
        }

        matchedPartnerId = candidateId;
        break; // Match mil gaya!
      }

      if (matchedPartnerId) {
        const roomId = `room_${matchedPartnerId}_${socket.id}`;

        // Dono sockets ko room me dalna (Across servers)
        io.in(matchedPartnerId).socketsJoin(roomId);
        socket.join(roomId);

        // Dono ko Match Found aur ek dusre ka profile data bhejo
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
        // Queue me dalo
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

  // Jab user Next dabata hai ya room chhodta hai
  socket.on('leaveChat', async () => {
    await redisClient.lrem('appie_waiting_queue', 0, socket.id);
    socket.rooms.forEach((roomId) => {
      if (roomId !== socket.id) {
        socket.to(roomId).emit('strangerDisconnected');
        socket.leave(roomId);
      }
    });
  });

  // Jab user App close karta hai ya internet jata hai
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
