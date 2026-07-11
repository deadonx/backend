const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');

const app = express();
app.use(cors());
const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"]
  }
});

// Firebase ke rooms node ki tarah memory (RAM) me rooms store karne ke liye
let activeRooms = {}; 

io.on('connection', (socket) => {
  console.log('User connected:', socket.id);

  // Jab user NEXT click kare ya pehli baar join kare
  socket.on('findStranger', () => {
    // 1. Pehle se kisi room me ho toh use saaf karo
    leaveCurrentRoom(socket);

    // 2. Dekho koi single banda kisi room me wait kar raha hai kya
    let foundRoomId = null;
    for (const roomId in activeRooms) {
      if (activeRooms[roomId].length === 1) { 
        foundRoomId = roomId; 
        break; 
      }
    }

    if (foundRoomId) {
      // Room mil gaya! Ghuus jao usme
      activeRooms[foundRoomId].push(socket.id);
      socket.join(foundRoomId);
      
      const callerId = activeRooms[foundRoomId][0]; 
      
      // Dono ko unka role aur room ID ek sath bhej do
      io.to(callerId).emit('matchFound', { roomId: foundRoomId, role: 'caller' });
      io.to(socket.id).emit('matchFound', { roomId: foundRoomId, role: 'callee' });
      
      console.log(`Matched! Room: ${foundRoomId}`);
    } else {
      // Koi khali room nahi mila toh naya room bana kar khud wait karo
      const newRoomId = 'room_' + socket.id + '_' + Date.now();
      activeRooms[newRoomId] = [socket.id]; 
      socket.join(newRoomId);
      console.log(`Created new room: ${newRoomId}`);
    }
  });

  // ✅ HANDSHAKE: Callee (baad me aane wala) bata raha hai ki uska camera/engine ready hai
  socket.on('calleeReady', (roomId) => {
    socket.to(roomId).emit('calleeReady');
  });

  // WebRTC Signaling Channels
  socket.on('webrtcOffer', (data) => socket.to(data.roomId).emit('webrtcOffer', data.offer));
  socket.on('webrtcAnswer', (data) => socket.to(data.roomId).emit('webrtcAnswer', data.answer));
  socket.on('iceCandidate', (data) => socket.to(data.roomId).emit('iceCandidate', data.candidate));

  // Jab user NEXT dabaye ya window/tab close kare
  socket.on('leaveChat', () => leaveCurrentRoom(socket));
  socket.on('disconnect', () => {
    leaveCurrentRoom(socket);
    console.log('User disconnected:', socket.id);
  });

  // Firebase style room delete logic: Ek bhi banda nikla toh room destroy!
  function leaveCurrentRoom(sock) {
    for (const roomId in activeRooms) {
      if (activeRooms[roomId].includes(sock.id)) {
        sock.to(roomId).emit('strangerDisconnected');
        sock.leave(roomId);
        delete activeRooms[roomId]; 
        console.log(`Room Destroyed: ${roomId}`);
        break;
      }
    }
  }
});

server.listen(3001, () => {
  console.log('Firebase-style Handshake Server running on port 3001');
});
