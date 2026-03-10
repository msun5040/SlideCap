const { contextBridge, ipcRenderer } = require('electron')

contextBridge.exposeInMainWorld('electronAPI', {
  selectDirectory: () => ipcRenderer.invoke('select-directory'),
  startBackend: (networkRoot) => ipcRenderer.invoke('start-backend', networkRoot),
  stopBackend: () => ipcRenderer.invoke('stop-backend'),
  getBackendPort: () => ipcRenderer.invoke('get-backend-port'),
  onBackendLog: (callback) => {
    const handler = (_event, msg) => callback(msg)
    ipcRenderer.on('backend-log', handler)
    return () => ipcRenderer.removeListener('backend-log', handler)
  },
  onBackendError: (callback) => {
    const handler = (_event, msg) => callback(msg)
    ipcRenderer.on('backend-error', handler)
    return () => ipcRenderer.removeListener('backend-error', handler)
  },
  isElectron: true,
  getPlatform: () => process.platform,
})
