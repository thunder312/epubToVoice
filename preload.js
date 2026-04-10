'use strict';

const { contextBridge, ipcRenderer } = require('electron');

// Safe wrapper to get filesystem path from a File object (drag-drop)
let _getFilePath;
try {
  const { webUtils } = require('electron');
  _getFilePath = f => webUtils.getPathForFile(f);
} catch {
  _getFilePath = f => f.path || '';  // fallback for Electron < 32
}

contextBridge.exposeInMainWorld('api', {
  // File path from drag-drop File object
  getFilePath: f => _getFilePath(f),

  // Dialogs
  openFiles:    ()    => ipcRenderer.invoke('dialog:openFiles'),
  openFolder:   ()    => ipcRenderer.invoke('dialog:openFolder'),
  openOutput:   ()    => ipcRenderer.invoke('dialog:openOutput'),

  // Resolve dropped paths (files or folders → epub list)
  resolvePaths: paths => ipcRenderer.invoke('resolve-paths', paths),

  // Python / voices
  getPythonStatus: () => ipcRenderer.invoke('get-python-status'),
  loadVoices:      () => ipcRenderer.invoke('load-voices'),

  // Document structure (for preview modal)
  getStructure: filePath => ipcRenderer.invoke('get-structure', filePath),

  // Language detection
  detectLanguage: filePath => ipcRenderer.invoke('detect-language', filePath),

  // Voice demo
  demoVoice: (voice, rate, volume) => ipcRenderer.invoke('demo-voice', { voice, rate, volume }),

  // Conversion
  startConversion: opts   => ipcRenderer.invoke('start-conversion', opts),
  cancelJob:       jobId  => ipcRenderer.invoke('cancel-job', jobId),

  // Shell
  revealPath: p => ipcRenderer.invoke('reveal-path', p),

  // Resume detection
  checkResumable: (filePath, outputDir) => ipcRenderer.invoke('check-resumable', filePath, outputDir || null),

  // Events (returns unsubscribe fn)
  onProgress: callback => {
    const handler = (_e, data) => callback(data);
    ipcRenderer.on('conversion-progress', handler);
    return () => ipcRenderer.removeListener('conversion-progress', handler);
  },
});
