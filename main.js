import { app, BrowserWindow, ipcMain } from 'electron';
import path from 'node:path';
import fs from 'node:fs';
import net from 'node:net';
import dgram from 'node:dgram';
import crypto from 'node:crypto';
import os from 'node:os';
import { fileURLToPath } from 'node:url';
import Hyperswarm from 'hyperswarm';
import b4a from 'b4a';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// ============================================================================
// CONSTANTES DO PROTOCOLO P2P MULTIPLEXADO (CANAL DE CONTROLE + STREAMS TCP)
// ============================================================================
const FRAME_CONTROL_JSON = 0x01; // Handshake (nickname, role, worldMotd) + Ping/Pong (3s)
const FRAME_TCP_OPEN     = 0x02; // Abertura de conexão TCP do Minecraft
const FRAME_TCP_DATA     = 0x03; // Dados brutos TCP do Minecraft (isolados do chat/ping)
const FRAME_TCP_CLOSE    = 0x04; // Fechamento seguro de stream TCP

const MC_MULTICAST_ADDR = '224.0.2.60';
const MC_MULTICAST_PORT = 4445;

let mainWindow = null;

// Estado Global da Sessão P2P
let swarm = null;
let currentMode = null; // 'host' | 'client'
let currentRoomCode = '';
let currentNickname = 'Steve';
let hostMinecraftPort = 25565;
let detectedWorldMotd = 'Mundo Minecraft LAN';

// Map<peerId, { conn, nickname, role, ping, pingTimer, decoderBuffer, streams: Map<streamId, net.Socket> }>
const activePeers = new Map();

// Recursos de Rede Locais
let hostLanDetectorSocket = null;
let clientTcpServer = null;
let clientLocalTcpPort = 25565;
let clientUdpAnnouncerSocket = null;
let clientUdpAnnouncerTimer = null;
let nextStreamId = 1;

// ============================================================================
// ARMAZENAMENTO DE HISTÓRICO DE SALAS (JSON PERSISTENTE EM userData)
// ============================================================================
function getHistoryFilePath() {
  const userDataDir = app.getPath('userData');
  return path.join(userDataDir, 'room-history.json');
}

function readRoomHistoryFromDisk() {
  try {
    const file = getHistoryFilePath();
    if (!fs.existsSync(file)) return [];
    const content = fs.readFileSync(file, 'utf-8');
    const parsed = JSON.parse(content);
    return Array.isArray(parsed) ? parsed : [];
  } catch (err) {
    console.error('[Histórico] Erro ao ler room-history.json:', err.message);
    return [];
  }
}

function writeRoomHistoryToDisk(historyArray) {
  try {
    const file = getHistoryFilePath();
    fs.mkdirSync(path.dirname(file), { recursive: true });
    fs.writeFileSync(file, JSON.stringify(historyArray, null, 2), 'utf-8');
    return true;
  } catch (err) {
    console.error('[Histórico] Erro ao salvar room-history.json:', err.message);
    return false;
  }
}

function upsertHistoryRecord({ roomCode, alias, mode, nickname, mcPort }) {
  const cleanCode = String(roomCode || '').trim().toLowerCase();
  if (!cleanCode) return readRoomHistoryFromDisk();

  const existing = readRoomHistoryFromDisk();
  const now = new Date().toISOString();
  const index = existing.findIndex(
    (item) => String(item.roomCode).toLowerCase() === cleanCode
  );

  let updated;
  if (index >= 0) {
    const prev = existing[index];
    const merged = {
      ...prev,
      roomCode: cleanCode,
      alias: alias && alias.trim() ? alias.trim() : prev.alias || `Sala ${cleanCode.slice(0, 10)}`,
      mode: mode || prev.mode || 'client',
      nickname: nickname && nickname.trim() ? nickname.trim() : prev.nickname || 'Steve',
      mcPort: Number(mcPort) || prev.mcPort || 25565,
      lastVisited: now,
      visitCount: (Number(prev.visitCount) || 1) + 1,
    };
    updated = [merged, ...existing.filter((_, idx) => idx !== index)];
  } else {
    const created = {
      roomCode: cleanCode,
      alias: alias && alias.trim() ? alias.trim() : `Sala ${cleanCode.slice(0, 10)}`,
      mode: mode || 'client',
      nickname: nickname && nickname.trim() ? nickname.trim() : 'Steve',
      mcPort: Number(mcPort) || 25565,
      lastVisited: now,
      visitCount: 1,
      pinned: false,
    };
    updated = [created, ...existing];
  }

  updated.sort((a, b) => {
    if (Boolean(a.pinned) !== Boolean(b.pinned)) return a.pinned ? -1 : 1;
    return new Date(b.lastVisited).getTime() - new Date(a.lastVisited).getTime();
  });

  writeRoomHistoryToDisk(updated);
  return updated;
}

// ============================================================================
// UTILITÁRIOS DE LOG E EVENTOS IPC PARA O RENDERER
// ============================================================================
function sendLog(level, category, message) {
  const entry = {
    id: crypto.randomUUID(),
    timestamp: new Date().toLocaleTimeString('pt-BR', { hour12: false }),
    level,
    category,
    message,
  };
  if (mainWindow && !mainWindow.isDestroyed()) {
    mainWindow.webContents.send('p2p:log', entry);
  }
}

function broadcastPeerList() {
  if (!mainWindow || mainWindow.isDestroyed()) return;

  const peers = [];
  peers.push({
    peerId: 'local-self',
    nickname: `${currentNickname} (Você)`,
    role: currentMode === 'host' ? 'Host' : 'Cliente',
    ping: 0,
    isSelf: true,
    activeStreams: 0,
  });

  for (const [peerId, peer] of activePeers.entries()) {
    peers.push({
      peerId,
      nickname: peer.nickname || `Peer-${peerId.slice(0, 6)}`,
      role: peer.role === 'host' ? 'Host' : 'Cliente',
      ping: peer.ping,
      isSelf: false,
      activeStreams: peer.streams ? peer.streams.size : 0,
    });
  }

  mainWindow.webContents.send('p2p:peers', peers);
}

function broadcastTunnelState(statusPayload) {
  if (mainWindow && !mainWindow.isDestroyed()) {
    mainWindow.webContents.send('p2p:state', statusPayload);
  }
}

// ============================================================================
// CODIFICAÇÃO E DECODIFICAÇÃO DE FRAMES MULTIPLEXADOS (9-BYTE HEADER)
// ============================================================================
function encodeFrame(type, streamId, payloadBuffer) {
  const header = b4a.allocUnsafe(9);
  header.writeUInt8(type, 0);
  header.writeUInt32BE(streamId >>> 0, 1);
  header.writeUInt32BE(payloadBuffer.length >>> 0, 5);
  return b4a.concat([header, payloadBuffer]);
}

function sendControlJson(conn, obj) {
  if (!conn || conn.destroyed) return;
  try {
    const jsonBuf = b4a.from(JSON.stringify(obj), 'utf8');
    conn.write(encodeFrame(FRAME_CONTROL_JSON, 0, jsonBuf));
  } catch (err) {
    sendLog('warn', 'P2P', `Falha ao enviar pacote de controle: ${err.message}`);
  }
}

function sendTcpFrame(conn, type, streamId, dataBuf = b4a.alloc(0)) {
  if (!conn || conn.destroyed) return;
  try {
    conn.write(encodeFrame(type, streamId, dataBuf));
  } catch (err) {
    sendLog('warn', 'TCP', `Erro ao escrever frame TCP #${streamId}: ${err.message}`);
  }
}

// ============================================================================
// INTERFACES DE REDE MULTICAST
// ============================================================================
function getLocalIPv4Interfaces() {
  const interfaces = os.networkInterfaces();
  const addresses = ['0.0.0.0', '127.0.0.1'];
  for (const name of Object.keys(interfaces)) {
    for (const netInfo of interfaces[name] || []) {
      if (netInfo.family === 'IPv4' && !netInfo.internal) {
        addresses.push(netInfo.address);
      }
    }
  }
  return Array.from(new Set(addresses));
}

// ============================================================================
// MODO A (HOST): DETECTOR UDP DO MINECRAFT
// ============================================================================
function startHostLanDetector() {
  stopHostLanDetector();

  try {
    const sock = dgram.createSocket({ type: 'udp4', reuseAddr: true });
    hostLanDetectorSocket = sock;

    sock.on('error', (err) => {
      sendLog('error', 'LAN', `Erro no detector UDP 4445: ${err.message}`);
    });

    sock.on('message', (msg, rinfo) => {
      const text = msg.toString('utf8');
      const motdMatch = text.match(/\[MOTD\]([\s\S]*?)\[\/MOTD\]/);
      const adMatch = text.match(/\[AD\](\d+)\[\/AD\]/);

      if (adMatch) {
        const port = parseInt(adMatch[1], 10);
        const motd = motdMatch ? motdMatch[1].trim() : 'Mundo Minecraft LAN';

        if (port > 0 && port <= 65535) {
          const changed = hostMinecraftPort !== port || detectedWorldMotd !== motd;
          hostMinecraftPort = port;
          detectedWorldMotd = motd;

          if (changed) {
            sendLog(
              'success',
              'LAN',
              `Mundo LAN detectado: "${motd}" na porta TCP ${port}`
            );
            if (mainWindow && !mainWindow.isDestroyed()) {
              mainWindow.webContents.send('p2p:lan-detected', {
                motd,
                port,
                address: rinfo.address,
              });
            }
            for (const peer of activePeers.values()) {
              sendControlJson(peer.conn, {
                type: 'world-update',
                motd: detectedWorldMotd,
                mcPort: hostMinecraftPort,
              });
            }
          }
        }
      }
    });

    sock.bind(MC_MULTICAST_PORT, () => {
      try {
        sock.setBroadcast(true);
        sock.setMulticastTTL(128);
        sock.setMulticastLoopback(true);

        const ifaces = getLocalIPv4Interfaces();
        for (const ifaceAddr of ifaces) {
          try {
            sock.addMembership(MC_MULTICAST_ADDR, ifaceAddr);
          } catch {
            // ignore
          }
        }
        sendLog(
          'info',
          'LAN',
          `Aguardando "Abrir para LAN" no Minecraft local (Porta 4445)...`
        );
      } catch (err) {
        sendLog('warn', 'LAN', `Aviso multicast: ${err.message}`);
      }
    });
  } catch (err) {
    sendLog('error', 'LAN', `Falha ao iniciar detector UDP: ${err.message}`);
  }
}

function stopHostLanDetector() {
  if (hostLanDetectorSocket) {
    try {
      hostLanDetectorSocket.close();
    } catch {
      // ignore
    }
    hostLanDetectorSocket = null;
  }
}

// ============================================================================
// MODO B (CLIENTE): SERVIDOR TCP LOCAL + BROADCAST LAN FALSO
// ============================================================================
function findHostPeer() {
  for (const peer of activePeers.values()) {
    if (peer.role === 'host') return peer;
  }
  return activePeers.values().next().value || null;
}

function startClientLocalTcpProxy(preferredPort = 25565) {
  return new Promise((resolve, reject) => {
    stopClientLocalTcpProxy();

    const server = net.createServer((minecraftSocket) => {
      minecraftSocket.setNoDelay(true);
      const hostPeer = findHostPeer();

      if (!hostPeer || !hostPeer.conn || hostPeer.conn.destroyed) {
        sendLog(
          'warn',
          'TCP',
          'Minecraft tentou conectar localmente, mas nenhum Host está ativo na sala P2P no momento.'
        );
        minecraftSocket.destroy();
        return;
      }

      const streamId = nextStreamId++;
      hostPeer.streams.set(streamId, minecraftSocket);
      sendLog(
        'info',
        'TCP',
        `Conexão do Minecraft recebida (Stream #${streamId}). Encaminhando para o Host...`
      );
      broadcastPeerList();

      sendTcpFrame(hostPeer.conn, FRAME_TCP_OPEN, streamId);

      minecraftSocket.on('data', (chunk) => {
        sendTcpFrame(hostPeer.conn, FRAME_TCP_DATA, streamId, chunk);
      });

      const cleanupStream = () => {
        if (hostPeer.streams.has(streamId)) {
          hostPeer.streams.delete(streamId);
          sendTcpFrame(hostPeer.conn, FRAME_TCP_CLOSE, streamId);
          broadcastPeerList();
        }
      };

      minecraftSocket.on('close', cleanupStream);
      minecraftSocket.on('error', (err) => {
        sendLog('warn', 'TCP', `Aviso no socket local (#${streamId}): ${err.message}`);
        cleanupStream();
      });
    });

    server.on('error', (err) => {
      if (err.code === 'EADDRINUSE' && preferredPort !== 0) {
        sendLog(
          'warn',
          'TCP',
          `Porta ${preferredPort} em uso. Alocando porta TCP livre automaticamente...`
        );
        server.listen(0, '127.0.0.1');
      } else {
        sendLog('error', 'TCP', `Falha no servidor TCP: ${err.message}`);
        reject(err);
      }
    });

    server.listen(preferredPort, '127.0.0.1', () => {
      const addr = server.address();
      clientLocalTcpPort = typeof addr === 'object' && addr ? addr.port : preferredPort;
      clientTcpServer = server;
      sendLog(
        'success',
        'TCP',
        `Proxy TCP Local pronto em 127.0.0.1:${clientLocalTcpPort}`
      );
      resolve(clientLocalTcpPort);
    });
  });
}

function stopClientLocalTcpProxy() {
  if (clientTcpServer) {
    try {
      clientTcpServer.close();
    } catch {
      // ignore
    }
    clientTcpServer = null;
  }
}

function startClientFakeLanBroadcast() {
  stopClientFakeLanBroadcast();

  try {
    const sock = dgram.createSocket({ type: 'udp4', reuseAddr: true });
    clientUdpAnnouncerSocket = sock;

    sock.on('error', (err) => {
      sendLog('warn', 'LAN', `Aviso no broadcast LAN UDP: ${err.message}`);
    });

    sock.bind(0, '0.0.0.0', () => {
      try {
        sock.setBroadcast(true);
        sock.setMulticastTTL(128);
        sock.setMulticastLoopback(true);
        try {
          sock.setMulticastInterface('0.0.0.0');
        } catch {
          try { sock.setMulticastInterface('127.0.0.1'); } catch {}
        }
      } catch (err) {
        sendLog('warn', 'LAN', `Erro flags UDP: ${err.message}`);
      }

      const broadcastPacket = () => {
        if (!clientUdpAnnouncerSocket) return;
        const hostPeer = findHostPeer();
        const hostName = hostPeer ? hostPeer.nickname : 'Host P2P';
        const message = `[MOTD]Sala P2P - ${hostName}[/MOTD][AD]${clientLocalTcpPort}[/AD]`;
        const payload = b4a.from(message, 'utf8');

        const interfacesToTry = ['0.0.0.0', '127.0.0.1'];
        for (const iface of interfacesToTry) {
          try { sock.setMulticastInterface(iface); } catch {}
          sock.send(payload, 0, payload.length, MC_MULTICAST_PORT, MC_MULTICAST_ADDR);
        }
      };

      broadcastPacket();
      clientUdpAnnouncerTimer = setInterval(broadcastPacket, 1500);
      sendLog(
        'success',
        'LAN',
        `Broadcast LAN Falso ativo em 224.0.2.60:4445 -> Porta ${clientLocalTcpPort}`
      );
    });
  } catch (err) {
    sendLog('error', 'LAN', `Falha ao iniciar broadcast LAN: ${err.message}`);
  }
}

function stopClientFakeLanBroadcast() {
  if (clientUdpAnnouncerTimer) {
    clearInterval(clientUdpAnnouncerTimer);
    clientUdpAnnouncerTimer = null;
  }
  if (clientUdpAnnouncerSocket) {
    try {
      clientUdpAnnouncerSocket.close();
    } catch {
      // ignore
    }
    clientUdpAnnouncerSocket = null;
  }
}

// ============================================================================
// CONEXÕES HYPERSWARM E TRATAMENTO P2P
// ============================================================================
function handleIncomingPeerConnection(conn, info) {
  const peerId = b4a.toString(info.publicKey, 'hex').slice(0, 12);
  conn.setKeepAlive(true, 2000);

  const peerState = {
    peerId,
    conn,
    nickname: `Jogador-${peerId.slice(0, 4)}`,
    role: currentMode === 'host' ? 'client' : 'host',
    ping: null,
    pingTimer: null,
    decoderBuffer: b4a.alloc(0),
    streams: new Map(),
  };

  activePeers.set(peerId, peerState);
  sendLog('success', 'P2P', `Túnel P2P direto estabelecido com peer [${peerId}]!`);

  sendControlJson(conn, {
    type: 'handshake',
    nickname: currentNickname,
    role: currentMode,
    motd: detectedWorldMotd,
    mcPort: hostMinecraftPort,
  });

  peerState.pingTimer = setInterval(() => {
    sendControlJson(conn, { type: 'ping', ts: Date.now() });
  }, 3000);

  sendControlJson(conn, { type: 'ping', ts: Date.now() });
  broadcastPeerList();

  conn.on('data', (chunk) => {
    peerState.decoderBuffer = b4a.concat([peerState.decoderBuffer, chunk]);

    while (peerState.decoderBuffer.length >= 9) {
      const frameType = peerState.decoderBuffer.readUInt8(0);
      const streamId = peerState.decoderBuffer.readUInt32BE(1);
      const payloadLength = peerState.decoderBuffer.readUInt32BE(5);

      if (payloadLength > 16 * 1024 * 1024) {
        sendLog('error', 'P2P', `Frame grande demais (${payloadLength} bytes). Resetando buffer.`);
        peerState.decoderBuffer = b4a.alloc(0);
        break;
      }

      if (peerState.decoderBuffer.length < 9 + payloadLength) {
        break;
      }

      const payload = peerState.decoderBuffer.subarray(9, 9 + payloadLength);
      peerState.decoderBuffer = peerState.decoderBuffer.subarray(9 + payloadLength);

      processMultiplexedFrame(peerState, frameType, streamId, payload);
    }
  });

  const cleanupPeer = (reason) => {
    if (!activePeers.has(peerId)) return;
    if (peerState.pingTimer) clearInterval(peerState.pingTimer);

    for (const [, tcpSocket] of peerState.streams.entries()) {
      try { tcpSocket.destroy(); } catch {}
    }
    peerState.streams.clear();
    activePeers.delete(peerId);

    sendLog('warn', 'P2P', `Peer ${peerState.nickname} [${peerId}] desconectou (${reason}).`);
    broadcastPeerList();
  };

  conn.on('close', () => cleanupPeer('conexão encerrada'));
  conn.on('error', (err) => cleanupPeer(err.message || 'erro de rede'));
}

function processMultiplexedFrame(peerState, frameType, streamId, payload) {
  if (frameType === FRAME_CONTROL_JSON) {
    try {
      const data = JSON.parse(b4a.toString(payload, 'utf8'));

      if (data.type === 'handshake') {
        peerState.nickname = data.nickname || peerState.nickname;
        peerState.role = data.role || peerState.role;
        if (data.motd) detectedWorldMotd = data.motd;
        sendLog(
          'info',
          'P2P',
          `Handshake concluído: "${peerState.nickname}" entrou na sala como ${
            peerState.role === 'host' ? 'HOST' : 'CLIENTE'
          }.`
        );
        broadcastPeerList();
      } else if (data.type === 'ping') {
        sendControlJson(peerState.conn, { type: 'pong', ts: data.ts });
      } else if (data.type === 'pong') {
        const rtt = Math.max(1, Date.now() - Number(data.ts));
        peerState.ping = rtt;
        broadcastPeerList();
      } else if (data.type === 'world-update') {
        if (data.motd) detectedWorldMotd = data.motd;
        sendLog('info', 'LAN', `Mundo LAN atualizado: "${detectedWorldMotd}"`);
      }
    } catch (err) {
      sendLog('warn', 'P2P', `Pacote inválido ignorado: ${err.message}`);
    }
    return;
  }

  if (frameType === FRAME_TCP_OPEN) {
    if (currentMode !== 'host') return;

    const targetPort = Number(hostMinecraftPort) || 25565;
    const mcServerSocket = net.connect({ host: '127.0.0.1', port: targetPort }, () => {
      mcServerSocket.setNoDelay(true);
      sendLog(
        'success',
        'TCP',
        `Pipe TCP #${streamId} conectado ao Minecraft (127.0.0.1:${targetPort}) para ${peerState.nickname}`
      );
    });

    peerState.streams.set(streamId, mcServerSocket);
    broadcastPeerList();

    mcServerSocket.on('data', (chunk) => {
      sendTcpFrame(peerState.conn, FRAME_TCP_DATA, streamId, chunk);
    });

    const closeHostPipe = () => {
      if (peerState.streams.has(streamId)) {
        peerState.streams.delete(streamId);
        sendTcpFrame(peerState.conn, FRAME_TCP_CLOSE, streamId);
        broadcastPeerList();
      }
    };

    mcServerSocket.on('close', closeHostPipe);
    mcServerSocket.on('error', (err) => {
      sendLog(
        'error',
        'TCP',
        `Falha ao conectar no Minecraft local (127.0.0.1:${targetPort}): ${err.message}. Clique em "Abrir para LAN" no jogo!`
      );
      closeHostPipe();
    });
    return;
  }

  if (frameType === FRAME_TCP_DATA) {
    const socket = peerState.streams.get(streamId);
    if (socket && !socket.destroyed) {
      socket.write(payload);
    }
    return;
  }

  if (frameType === FRAME_TCP_CLOSE) {
    const socket = peerState.streams.get(streamId);
    if (socket) {
      peerState.streams.delete(streamId);
      try { socket.destroy(); } catch {}
      broadcastPeerList();
    }
  }
}

// ============================================================================
// INICIALIZAÇÃO DA SESSÃO P2P (HYPERSWARM DHT COM FORÇA DE RELAY E NORMALIZAÇÃO)
// ============================================================================
async function stopTunnelSession() {
  stopHostLanDetector();
  stopClientLocalTcpProxy();
  stopClientFakeLanBroadcast();

  for (const [, peer] of activePeers.entries()) {
    if (peer.pingTimer) clearInterval(peer.pingTimer);
    for (const [, s] of peer.streams.entries()) {
      try { s.destroy(); } catch {}
    }
    try { peer.conn.destroy(); } catch {}
  }
  activePeers.clear();

  if (swarm) {
    try {
      await swarm.destroy();
    } catch {
      // ignore
    }
    swarm = null;
  }

  currentMode = null;
  broadcastPeerList();
  broadcastTunnelState({
    connected: false,
    mode: null,
    roomCode: '',
    topicHex: '',
    localDirectConnect: '',
  });
}

async function startTunnelSession({ mode, roomCode, alias, nickname, mcPort }) {
  await stopTunnelSession();

  currentMode = mode === 'host' ? 'host' : 'client';
  
  // Limpa espaços invisíveis, quebras de linha e transforma em minúsculas
  currentRoomCode = String(roomCode || '')
    .trim()
    .toLowerCase()
    .replace(/\s+/g, '');

  currentNickname = String(nickname || 'Steve').trim() || 'Steve';
  if (mcPort && Number(mcPort) > 0) {
    hostMinecraftPort = Number(mcPort);
  }

  // Hash SHA-256 de 32 bytes exatos
  const topicBuffer = crypto.createHash('sha256').update(currentRoomCode, 'utf8').digest();
  const topicHex = b4a.toString(topicBuffer, 'hex');

  sendLog('info', 'DHT', `Inicializando túnel P2P...`);
  sendLog('info', 'DHT', `Sala limpa: "${currentRoomCode}" -> Tópico SHA-256: ${topicHex.slice(0, 24)}...`);

  // Configuração reforçada para atravessar CGNAT / Firewalls domésticos
  swarm = new Hyperswarm({
    maxPeers: 32,
    firewalled: true,
  });

  swarm.on('connection', (conn, info) => {
    handleIncomingPeerConnection(conn, info);
  });

  let localEndpoint = `127.0.0.1:${hostMinecraftPort}`;

  if (currentMode === 'host') {
    startHostLanDetector();
    sendLog('info', 'SYS', `Modo HOST ativado para o Minecraft local.`);
  } else {
    const boundPort = await startClientLocalTcpProxy(25565);
    localEndpoint = `127.0.0.1:${boundPort}`;
    startClientFakeLanBroadcast();
  }

  const discovery = swarm.join(topicBuffer, {
    server: true,
    client: true,
  });

  discovery.flushed().then(() => {
    sendLog('success', 'DHT', `Tópico anunciado globalmente! Aguardando amigos na sala...`);
  }).catch((err) => {
    sendLog('warn', 'DHT', `Aviso na DHT: ${err.message}`);
  });

  const updatedHistory = upsertHistoryRecord({
    roomCode: currentRoomCode,
    alias,
    mode: currentMode,
    nickname: currentNickname,
    mcPort: hostMinecraftPort,
  });

  broadcastPeerList();
  const statePayload = {
    connected: true,
    mode: currentMode,
    roomCode: currentRoomCode,
    topicHex,
    localDirectConnect: localEndpoint,
    mcPort: currentMode === 'host' ? hostMinecraftPort : clientLocalTcpPort,
    history: updatedHistory,
  };
  broadcastTunnelState(statePayload);
  return statePayload;
}

// ============================================================================
// IPC HANDLERS
// ============================================================================
function registerIpcHandlers() {
  ipcMain.handle('p2p:start', async (_event, config) => {
    return await startTunnelSession(config);
  });

  ipcMain.handle('p2p:stop', async () => {
    await stopTunnelSession();
    sendLog('info', 'SYS', 'Túnel P2P encerrado pelo usuário.');
    return { connected: false };
  });

  ipcMain.handle('p2p:set-mc-port', (_event, portNumber) => {
    const parsed = parseInt(portNumber, 10);
    if (!Number.isNaN(parsed) && parsed > 0 && parsed <= 65535) {
      hostMinecraftPort = parsed;
      sendLog('info', 'LAN', `Porta TCP definida para: ${parsed}`);
      return { ok: true, port: hostMinecraftPort };
    }
    return { ok: false };
  });

  ipcMain.handle('p2p:detect-lan-now', () => {
    startHostLanDetector();
    sendLog('info', 'LAN', 'Reiniciando varredura UDP na porta 4445...');
    return { ok: true, port: hostMinecraftPort, motd: detectedWorldMotd };
  });

  ipcMain.handle('history:get', () => readRoomHistoryFromDisk());
  ipcMain.handle('history:save', (_event, entry) => upsertHistoryRecord(entry));
  ipcMain.handle('history:delete', (_event, roomCode) => {
    const existing = readRoomHistoryFromDisk();
    const filtered = existing.filter(
      (item) => String(item.roomCode).toLowerCase() !== String(roomCode).toLowerCase()
    );
    writeRoomHistoryToDisk(filtered);
    return filtered;
  });

  ipcMain.handle('history:toggle-pin', (_event, roomCode) => {
    const existing = readRoomHistoryFromDisk();
    const updated = existing.map((item) => {
      if (String(item.roomCode).toLowerCase() === String(roomCode).toLowerCase()) {
        return { ...item, pinned: !item.pinned };
      }
      return item;
    });
    updated.sort((a, b) => {
      if (Boolean(a.pinned) !== Boolean(b.pinned)) return a.pinned ? -1 : 1;
      return new Date(b.lastVisited).getTime() - new Date(a.lastVisited).getTime();
    });
    writeRoomHistoryToDisk(updated);
    return updated;
  });

  ipcMain.handle('history:clear', () => {
    writeRoomHistoryToDisk([]);
    return [];
  });
}

// ============================================================================
// JANELA PRINCIPAL
// ============================================================================
function createMainWindow() {
  mainWindow = new BrowserWindow({
    width: 1360,
    height: 860,
    minWidth: 1024,
    minHeight: 720,
    title: 'CraftTunnel P2P — Minecraft LAN VPN',
    backgroundColor: '#0b0e14',
    autoHideMenuBar: true,
    webPreferences: {
      preload: path.join(__dirname, 'preload.cjs'),
      contextIsolation: true,
      nodeIntegration: false,
      sandbox: false,
    },
  });

  mainWindow.loadFile(path.join(__dirname, 'dist', 'index.html'));
}

app.whenReady().then(() => {
  registerIpcHandlers();
  createMainWindow();

  app.on('activate', () => {
    if (BrowserWindow.getAllWindows().length === 0) {
      createMainWindow();
    }
  });
});

app.on('window-all-closed', async () => {
  await stopTunnelSession();
  if (process.platform !== 'darwin') {
    app.quit();
  }
});
