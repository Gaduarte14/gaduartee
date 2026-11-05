// preload.cjs — precisa ter extensão .cjs porque o package.json tem "type": "module"
const { contextBridge, ipcRenderer } = require('electron');

/**
 * Exposição segura via contextBridge entre o Processo Principal (Node.js / Hyperswarm)
 * e a Interface do Usuário (Renderer) sem habilitar nodeIntegration direta.
 */
contextBridge.exposeInMainWorld('craftTunnelAPI', {
  isElectron: true,

  // Controles do Túnel P2P Hyperswarm
  startTunnel: (config) => ipcRenderer.invoke('p2p:start', config),
  stopTunnel: () => ipcRenderer.invoke('p2p:stop'),
  setMinecraftPort: (port) => ipcRenderer.invoke('p2p:set-mc-port', port),
  detectLanNow: () => ipcRenderer.invoke('p2p:detect-lan-now'),

  // Gerenciamento de Histórico de Salas (Salvo em room-history.json + sincronizado com UI)
  getRoomHistory: () => ipcRenderer.invoke('history:get'),
  saveRoomToHistory: (entry) => ipcRenderer.invoke('history:save', entry),
  deleteRoomFromHistory: (roomCode) => ipcRenderer.invoke('history:delete', roomCode),
  togglePinRoomHistory: (roomCode) => ipcRenderer.invoke('history:toggle-pin', roomCode),
  clearRoomHistory: () => ipcRenderer.invoke('history:clear'),

  // Listeners de Eventos em Tempo Real vindos do Processo Principal
  onLog: (callback) => {
    const listener = (_event, entry) => callback(entry);
    ipcRenderer.on('p2p:log', listener);
    return () => ipcRenderer.removeListener('p2p:log', listener);
  },

  onPeersUpdate: (callback) => {
    const listener = (_event, peers) => callback(peers);
    ipcRenderer.on('p2p:peers', listener);
    return () => ipcRenderer.removeListener('p2p:peers', listener);
  },

  onStateChange: (callback) => {
    const listener = (_event, state) => callback(state);
    ipcRenderer.on('p2p:state', listener);
    return () => ipcRenderer.removeListener('p2p:state', listener);
  },

  onLanDetected: (callback) => {
    const listener = (_event, lanInfo) => callback(lanInfo);
    ipcRenderer.on('p2p:lan-detected', listener);
    return () => ipcRenderer.removeListener('p2p:lan-detected', listener);
  },
});
