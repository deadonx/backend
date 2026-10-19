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
const REDIS_URL = process.env.REDIS_URL || "rediss://default:gQAAAAAAAcMpAAIgcDI3M2UyMDQwNGJhN2M0MWNhYmVlNGQzYTlmY2EyMzlmMQ@credible-sunbird-115497.upstash.io:6379";

// Render aur Upstash ke secure SSL connection ke liye tls option zaroori hai
const redisOptions = {
    tls: { rejectUnauthorized: false },
    retryStrategy: (times) => Math.min(times * 50, 2000)
};

const pubClient = new Redis(REDIS_URL, redisOptions);
const subClient = pubClient.duplicate();
const redisClient = new Redis(REDIS_URL, redisOptions); // General data (Queue) ke liye

const io = new Server(server, {
  cors: { origin: "*", methods: ["GET", "POST"] }
});

// 🔥 2. LINK SOCKET.IO WITH REDIS (For Multi-Server) 🔥
io.adapter(createAdapter(pubClient, subClient));

pubClient.on('connect', () => console.log('🔥 Redis Pub/Sub & Master Server Connected!'));

io.on('connection', (socket) => {
  console.log('User connected:', socket.id);

  // ==========================================
  // 🔥 THE REDIS MATCHMAKING SYSTEM 🔥
  // ==========================================
  socket.on('findStranger', async () => {
    try {
      // 1. Redis ke queue se ek banda nikalo (LPOP)
      const partnerId = await redisClient.lpop('appie_waiting_queue');

      if (partnerId && partnerId !== socket.id) {
        // MATCH MIL GAYA! 
        const roomId = 'room_' + partnerId + '_' + socket.id;
        
        // Dono ko 'matchFound' bhej do
        io.to(partnerId).emit('matchFound', { roomId: roomId, role: 'caller' });
        io.to(socket.id).emit('matchFound', { roomId: roomId, role: 'callee' });
        
        console.log(`Matched! Room: ${roomId}`);
      } else {
        // KOI KHALI NAHI HAI: Queue me lagao (RPUSH)
        await redisClient.rpush('appie_waiting_queue', socket.id);
        await redisClient.expire('appie_waiting_queue', 60); 
        console.log(`User ${socket.id} waiting in Redis queue...`);
      }
    } catch (err) {
      console.error("Redis Matchmaking Error:", err);
    }
  });

  // Socket room join karna
  socket.on('joinRoom', (roomId) => {
    socket.join(roomId);
  });

  // Callee ready signal
  socket.on('calleeReady', (roomId) => {
    socket.to(roomId).emit('calleeReady');
  });

  // ==========================================
  // 🔥 WEBRTC VIDEO SIGNALING (With Name & DP Support)
  // ==========================================
  socket.on('webrtcOffer', (data) => {
    // data ke andar offer, peerName, peerDp sab kuch aayega aur seedha pass ho jayega
    socket.to(data.roomId).emit('webrtcOffer', data);
  });

  socket.on('webrtcAnswer', (data) => {
    socket.to(data.roomId).emit('webrtcAnswer', data);
  });

  socket.on('iceCandidate', (data) => {
    socket.to(data.roomId).emit('iceCandidate', data);
  });

  // ==========================================
  // 🔥 DISCONNECT & SKIP LOGIC
  // ==========================================
  socket.on('leaveChat', async (roomId) => {
    if (roomId) {
      socket.to(roomId).emit('strangerDisconnected');
      socket.leave(roomId);
    }
    await redisClient.lrem('appie_waiting_queue', 0, socket.id);
  });

  socket.on('disconnect', async () => {
    console.log('User disconnected:', socket.id);
    await redisClient.lrem('appie_waiting_queue', 0, socket.id);
    
    // Agar kisi room me tha toh stranger ko notify kar do
    socket.rooms.forEach(roomId => {
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
