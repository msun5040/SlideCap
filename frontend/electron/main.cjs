const { app, BrowserWindow, ipcMain, dialog } = require('electron')
const path = require('path')
const { spawn, execFile } = require('child_process')
const http = require('http')
const fs = require('fs')

let mainWindow
let backendProcess
let backendPort = 8000

function createWindow() {
  mainWindow = new BrowserWindow({
    width: 1200,
    height: 800,
    minWidth: 800,
    minHeight: 600,
    titleBarStyle: process.platform === 'darwin' ? 'hiddenInset' : 'default',
    trafficLightPosition: { x: 16, y: 16 },
    backgroundColor: '#ffffff',
    show: false,
    webPreferences: {
      nodeIntegration: false,
      contextIsolation: true,
      preload: path.join(__dirname, 'preload.cjs'),
    },
  })

  const isDev = !app.isPackaged

  if (isDev) {
    mainWindow.loadURL('http://localhost:5173')
  } else {
    mainWindow.loadFile(path.join(__dirname, '../dist/index.html'))
  }

  mainWindow.once('ready-to-show', () => {
    mainWindow.show()
  })

  mainWindow.on('closed', () => {
    mainWindow = null
  })
}

// -------------------------------------------------------
// Locate the bundled backend executable (PyInstaller output)
// In production: resources/backend/slidecap-backend[.exe]
// -------------------------------------------------------
function findBundledBackend() {
  const exeName = process.platform === 'win32' ? 'slidecap-backend.exe' : 'slidecap-backend'
  const candidates = [
    // electron-builder extraResources
    path.join(process.resourcesPath || '', 'backend', 'slidecap-backend', exeName),
    path.join(process.resourcesPath || '', 'backend', exeName),
    // Dev: if someone ran pyinstaller locally
    path.join(__dirname, '..', '..', 'backend', 'dist', 'slidecap-backend', exeName),
  ]
  for (const p of candidates) {
    if (fs.existsSync(p)) return p
  }
  return null
}

// Find python executable for dev mode fallback
function findPythonCandidates() {
  return process.platform === 'win32'
    ? ['python', 'python3', 'py']
    : ['python3', 'python']
}

// Check if backend is responding
function checkBackendHealth(port) {
  return new Promise((resolve) => {
    const req = http.get(`http://127.0.0.1:${port}/health`, (res) => {
      resolve(res.statusCode === 200)
    })
    req.on('error', () => resolve(false))
    req.setTimeout(3000, () => {
      req.destroy()
      resolve(false)
    })
  })
}

// Wait for backend to respond
async function waitForBackend(port, maxAttempts = 60) {
  for (let i = 0; i < maxAttempts; i++) {
    const healthy = await checkBackendHealth(port)
    if (healthy) return true
    await new Promise((r) => setTimeout(r, 1000))
  }
  return false
}

function sendLog(msg) {
  if (mainWindow && !mainWindow.isDestroyed()) {
    mainWindow.webContents.send('backend-log', msg)
  }
}

function sendError(msg) {
  if (mainWindow && !mainWindow.isDestroyed()) {
    mainWindow.webContents.send('backend-error', msg)
  }
}

// -------------------------------------------------------
// IPC: Select directory
// -------------------------------------------------------
ipcMain.handle('select-directory', async () => {
  const result = await dialog.showOpenDialog(mainWindow, {
    properties: ['openDirectory'],
    title: 'Select Network Root Directory',
    message: 'Choose the root directory containing your slides/ folder',
  })
  if (result.canceled || result.filePaths.length === 0) return null
  return result.filePaths[0]
})

// -------------------------------------------------------
// IPC: Start backend
// -------------------------------------------------------
ipcMain.handle('start-backend', async (_event, networkRoot) => {
  // Kill existing backend if running
  if (backendProcess) {
    backendProcess.kill()
    backendProcess = null
    await new Promise((r) => setTimeout(r, 500))
  }

  const env = {
    ...process.env,
    NETWORK_ROOT: networkRoot,
    PYTHONUNBUFFERED: '1',
  }

  // --- Try bundled backend first (production) ---
  const bundledExe = findBundledBackend()
  if (bundledExe) {
    sendLog(`Starting bundled backend: ${bundledExe}`)
    try {
      backendProcess = execFile(bundledExe, [], {
        env,
        stdio: ['pipe', 'pipe', 'pipe'],
      })

      backendProcess.stdout.on('data', (d) => sendLog(d.toString()))
      backendProcess.stderr.on('data', (d) => sendLog(d.toString()))
      backendProcess.on('error', (e) => sendError(e.message))
      backendProcess.on('exit', (code) => {
        sendError(`Backend exited (code ${code})`)
        backendProcess = null
      })

      sendLog('Waiting for backend to be ready...')
      const ready = await waitForBackend(backendPort)
      if (ready) {
        return { success: true, port: backendPort }
      } else {
        return { success: false, error: 'Backend started but did not respond. Check logs above.' }
      }
    } catch (err) {
      return { success: false, error: `Failed to start bundled backend: ${err.message}` }
    }
  }

  // --- Fallback: use system Python (dev mode) ---
  sendLog('No bundled backend found, trying system Python...')
  const pythonCandidates = findPythonCandidates()
  const backendDir = path.join(__dirname, '../../backend')

  for (const pythonCmd of pythonCandidates) {
    try {
      sendLog(`Trying: ${pythonCmd} -m uvicorn ...`)
      backendProcess = spawn(
        pythonCmd,
        ['-m', 'uvicorn', 'app.main:app', '--host', '127.0.0.1', '--port', String(backendPort)],
        {
          cwd: backendDir,
          env,
          stdio: ['pipe', 'pipe', 'pipe'],
          shell: process.platform === 'win32',
        }
      )

      backendProcess.stdout.on('data', (d) => sendLog(d.toString()))
      backendProcess.stderr.on('data', (d) => sendLog(d.toString()))
      backendProcess.on('error', (e) => {
        sendError(`${pythonCmd} error: ${e.message}`)
      })
      backendProcess.on('exit', (code) => {
        sendError(`Backend exited (code ${code})`)
        backendProcess = null
      })

      // Wait briefly to see if it crashes immediately
      await new Promise((r) => setTimeout(r, 2000))
      if (!backendProcess || backendProcess.killed) continue

      sendLog('Waiting for backend to be ready...')
      const ready = await waitForBackend(backendPort)
      if (ready) {
        return { success: true, port: backendPort }
      }
    } catch {
      // Try next candidate
    }
  }

  return {
    success: false,
    error: 'Could not start backend. Make sure Python 3.9+ is installed with dependencies (pip install -r requirements.txt), or use the pre-built package.',
  }
})

// -------------------------------------------------------
// IPC: Stop backend
// -------------------------------------------------------
ipcMain.handle('stop-backend', async () => {
  if (backendProcess) {
    backendProcess.kill()
    backendProcess = null
  }
  return true
})

ipcMain.handle('get-backend-port', () => backendPort)

// -------------------------------------------------------
// App lifecycle
// -------------------------------------------------------
app.whenReady().then(createWindow)

app.on('window-all-closed', () => {
  killBackend()
  if (process.platform !== 'darwin') app.quit()
})

app.on('activate', () => {
  if (mainWindow === null) createWindow()
})

app.on('before-quit', killBackend)

function killBackend() {
  if (backendProcess) {
    backendProcess.kill()
    backendProcess = null
  }
}
