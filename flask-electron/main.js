const { app, BrowserWindow } = require("electron");
const path = require("path");
const { spawn } = require("child_process");
const http = require("http");

let mainWindow;
let backend;

const isDev = !app.isPackaged;

// Resolve backend exe path in dev vs packaged app
function getBackendPath() {
  if (isDev) {
    return path.join(__dirname, "backend", "main.exe"); // 🔹 changed from app.exe → main.exe
  }
  // in packaged app, resources are under process.resourcesPath
  return path.join(process.resourcesPath, "backend", "main.exe"); // 🔹 changed
}

function waitForBackend(url, onUp) {
  const check = () => {
    const req = http.get(url, (res) => {
      // If Flask responds at all, proceed
      if (res.statusCode >= 200 && res.statusCode < 500) {
        onUp();
      } else {
        setTimeout(check, 1000);
      }
      res.resume();
    });
    req.on("error", () => setTimeout(check, 1000));
  };
  check();
}

function createWindow() {
  mainWindow = new BrowserWindow({
    width: 1200,
    height: 800,
    webPreferences: { nodeIntegration: false },
  });

  mainWindow.loadURL("http://127.0.0.1:5000/");

  mainWindow.on("closed", () => {
    mainWindow = null;
  });
}

app.on("ready", () => {
  // 1) start Flask
  const backendPath = getBackendPath();
  console.log("Launching backend from:", backendPath);

  backend = spawn(backendPath, [], {
    windowsHide: true,
    detached: false,
  });

  backend.stdout?.on("data", (d) => console.log(`Flask: ${d}`));
  backend.stderr?.on("data", (d) => console.error(`Flask ERR: ${d}`));
  backend.on("close", (code) => console.log(`Flask exited: ${code}`));

  // 2) wait until backend is reachable, then open window
  waitForBackend("http://127.0.0.1:5000/", createWindow);
});

app.on("window-all-closed", () => {
  if (process.platform !== "darwin") app.quit();
  if (backend && !backend.killed) {
    try { backend.kill(); } catch {}
  }
});

app.on("activate", () => {
  if (mainWindow === null) createWindow();
});
