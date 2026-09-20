const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');

// 🔥 NAYA 1: Firebase Admin import karo
const admin = require('firebase-admin');

// 🔥 NAYA 2: Service Account Key (Firebase se jo download ki thi)
const serviceAccount = require('./serviceAccountKey.json');

// 🔥 NAYA 3: Firebase ko start karo
admin.initializeApp({
  credential: admin.credential.cert(serviceAccount)
});

const app = express();
app.use(cors());

// 🔥 NAYA 4: JSON format padhne ke liye (Notification data aayega isme)
app.use(express.json());

const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"]
  }
});

// ==============================================================
// 🔥 NAYA 5: NOTIFICATION WALA RASTA (POST API) 🔥
// ==============================================================
app.post('/send-notification', async (req, res) => {
  const { token, title, body } = req.body;

  if (!token) {
    return res.status(400).send("Token is required");
  }

  const message = {
    notification: {
      title: title,
      body: body
    },
    token: token
  };

  try {
    const response = await admin.messaging().send(message);
    console.log("Notification sent successfully:", response);
    res.status(200).send("Sent!");
  } catch (error) {
    console.log("Error sending notification:", error);
    res.status(500).send(error.message);
  }
});

// ==============================================================
// PURANA WEBRTC (VIDEO CALL) WALA CODE (Ekdum safe aur same hai)
// ==============================================================
let activeRooms = {}; 

io.on('connection', (socket) => {
  console.log('User connected:', socket.id);

  socket.on('findStranger', () => {
    leaveCurrentRoom(socket);

    let foundRoomId = null;
    for (const roomId in activeRooms) {
      if (activeRooms[roomId].length === 1) { 
        foundRoomId = roomId; 
        break; 
      }
    }

    if (foundRoomId) {
      activeRooms[foundRoomId].push(socket.id);
      socket.join(foundRoomId);
      
      const callerId = activeRooms[foundRoomId][0]; 
      
      io.to(callerId).emit('matchFound', { roomId: foundRoomId, role: 'caller' });
      io.to(socket.id).emit('matchFound', { roomId: foundRoomId, role: 'callee' });
      
      console.log(`Matched! Room: ${foundRoomId}`);
    } else {
      const newRoomId = 'room_' + socket.id + '_' + Date.now();
      activeRooms[newRoomId] = [socket.id]; 
      socket.join(newRoomId);
      console.log(`Created new room: ${newRoomId}`);
    }
  });

  socket.on('calleeReady', (roomId) => {
    socket.to(roomId).emit('calleeReady');
  });

  socket.on('webrtcOffer', (data) => socket.to(data.roomId).emit('webrtcOffer', data.offer));
  socket.on('webrtcAnswer', (data) => socket.to(data.roomId).emit('webrtcAnswer', data.answer));
  socket.on('iceCandidate', (data) => socket.to(data.roomId).emit('iceCandidate', data.candidate));

  socket.on('leaveChat', () => leaveCurrentRoom(socket));
  socket.on('disconnect', () => {
    leaveCurrentRoom(socket);
    console.log('User disconnected:', socket.id);
  });

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

// 🔥 NAYA 6: Render ke liye process.env.PORT jaruri hota hai
const PORT = process.env.PORT || 3001;
server.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
