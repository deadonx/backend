const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const Redis = require('ioredis');
const { createAdapter } = require('@socket.io/redis-adapter');

const app = express();
app.use(cors());
const server = http.createServer(app);

// 🔥 1. REDIS SETUP 🔥
// Yahan apna Upstash wala REDIS_URL daaliye
const REDIS_URL = process.env.REDIS_URL || "rediss://default:AASDASD123123@cool-tiger-32000.upstash.io:32000";

const pubClient = new Redis(REDIS_URL);
const subClient = pubClient.duplicate();
const redisClient = new Redis(REDIS_URL); // General data (EXP, Queue) ke liye

const io = new Server(server, {
  cors: { origin: "*", methods: ["GET", "POST"] }
});

// 🔥 2. LINK SOCKET.IO WITH REDIS (For Multi-Server) 🔥
// Is ek line se Server 1 aur Server 2 aapas me baat kar payenge
io.adapter(createAdapter(pubClient, subClient));

pubClient.on('connect', () => console.log('🔥 Redis Pub/Sub Connected!'));

io.on('connection', (socket) => {
  console.log('User connected:', socket.id);

  // ==========================================
  // 🔥 THE NEW REDIS MATCHMAKING SYSTEM 🔥
  // ==========================================
  socket.on('findStranger', async () => {
    try {
      // 1. Redis ke 'waiting_queue' se ek banda nikalo (LPOP)
      const partnerId = await redisClient.lpop('appie_waiting_queue');

      if (partnerId && partnerId !== socket.id) {
        // MATCH MIL GAYA! 
        const roomId = 'room_' + partnerId + '_' + socket.id;
        
        // Dono ko 'matchFound' bhej do (Adapter automatically Server 1 ya 2 dhund lega)
        io.to(partnerId).emit('matchFound', { roomId: roomId, role: 'caller' });
        io.to(socket.id).emit('matchFound', { roomId: roomId, role: 'callee' });
        
        console.log(`Matched! Room: ${roomId}`);
      } else {
        // KOI KHALI NAHI HAI: Khud ko line (queue) me lagao (RPUSH)
        await redisClient.rpush('appie_waiting_queue', socket.id);
        
        // Agar user app band kar de, toh 60 sec baad wo line se khud hat jayega
        await redisClient.expire('appie_waiting_queue', 60); 
        console.log(`User ${socket.id} waiting in Redis queue...`);
      }
    } catch (err) {
      console.error("Redis Matchmaking Error:", err);
    }
  });

  // Jab Android app se 'matchFound' aaye, tab actually Socket room me join karna hai
  socket.on('joinRoom', (roomId) => {
    socket.join(roomId);
  });

  // ✅ HANDSHAKE: Callee (baad me aane wala) bata raha hai ki uska camera ready hai
  socket.on('calleeReady', (roomId) => {
    socket.to(roomId).emit('calleeReady');
  });

  // ==========================================
  // 🔥 WEBRTC VIDEO SIGNALING (Remains Same)
  // ==========================================
  socket.on('webrtcOffer', (data) => socket.to(data.roomId).emit('webrtcOffer', data.offer));
  socket.on('webrtcAnswer', (data) => socket.to(data.roomId).emit('webrtcAnswer', data.answer));
  socket.on('iceCandidate', (data) => socket.to(data.roomId).emit('iceCandidate', data.candidate));

  // ==========================================
  // 🔥 DISCONNECT & SKIP LOGIC
  // ==========================================
  socket.on('leaveChat', (roomId) => {
    socket.to(roomId).emit('strangerDisconnected');
    socket.leave(roomId);
  });

  socket.on('disconnect', async () => {
    console.log('User disconnected:', socket.id);
    // Queue me tha toh nikalne ki koshish karega (halaki 60 sec me khud delete ho jayega)
    await redisClient.lrem('appie_waiting_queue', 0, socket.id);
  });
});

const PORT = process.env.PORT || 3001;
server.listen(PORT, () => {
  console.log(`🚀 Appie Redis Master Server running on port ${PORT}`);
});
