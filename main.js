'use strict';

const { app, BrowserWindow, ipcMain, dialog, shell } = require('electron');
const path  = require('path');
const fs    = require('fs');
const { spawn } = require('child_process');
const archiver  = require('archiver');

// ---------------------------------------------------------------------------
// Window
// ---------------------------------------------------------------------------

let mainWindow;

function createWindow() {
  mainWindow = new BrowserWindow({
    width: 1100,
    height: 740,
    minWidth: 860,
    minHeight: 600,
    backgroundColor: '#0f0f1a',
    title: 'EPUB to Voice',
    webPreferences: {
      preload: path.join(__dirname, 'preload.js'),
      contextIsolation: true,
      nodeIntegration: false,
      sandbox: false,
    },
  });
  mainWindow.loadFile(path.join(__dirname, 'renderer', 'index.html'));
}

app.whenReady().then(async () => {
  createWindow();
  app.on('activate', () => {
    if (BrowserWindow.getAllWindows().length === 0) createWindow();
  });
});

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') app.quit();
});

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Path to epub_to_voice.py – works both in dev and packaged. */
function getScriptPath() {
  return app.isPackaged
    ? path.join(process.resourcesPath, 'epub_to_voice.py')
    : path.join(__dirname, 'epub_to_voice.py');
}

/** Try to find a working python executable. */
async function detectPython() {
  for (const cmd of ['python', 'python3', 'py']) {
    const ok = await new Promise(resolve => {
      const p = spawn(cmd, ['--version'], { shell: true });
      p.on('close', code => resolve(code === 0));
      p.on('error', ()  => resolve(false));
    });
    if (ok) return cmd;
  }
  return null;
}

const SUPPORTED_EXT = new Set(['.epub', '.pdf', '.txt', '.docx', '.doc']);

/** Recursively collect all supported input files under dir. */
function findEpubs(dir) {
  const results = [];
  try {
    for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
      const full = path.join(dir, entry.name);
      if (entry.isDirectory()) results.push(...findEpubs(full));
      else if (SUPPORTED_EXT.has(path.extname(entry.name).toLowerCase())) results.push(full);
    }
  } catch { /* ignore unreadable dirs */ }
  return results;
}

// ---------------------------------------------------------------------------
// Python detection (cached after first call)
// ---------------------------------------------------------------------------

let pythonCmdCache = undefined;

async function getPython() {
  if (pythonCmdCache === undefined) pythonCmdCache = await detectPython();
  return pythonCmdCache;
}

// ---------------------------------------------------------------------------
// IPC – dialogs
// ---------------------------------------------------------------------------

ipcMain.handle('dialog:openFiles', async () => {
  const { canceled, filePaths } = await dialog.showOpenDialog(mainWindow, {
    title: 'Select EPUB file(s)',
    properties: ['openFile', 'multiSelections'],
    filters: [
      { name: 'Unterstützte Formate', extensions: ['epub', 'pdf', 'txt', 'docx', 'doc'] },
      { name: 'EPUB',                 extensions: ['epub'] },
      { name: 'PDF',                  extensions: ['pdf'] },
      { name: 'Text',                 extensions: ['txt'] },
      { name: 'Word-Dokument',        extensions: ['docx', 'doc'] },
    ],
  });
  return canceled ? [] : filePaths;
});

ipcMain.handle('dialog:openFolder', async () => {
  const { canceled, filePaths } = await dialog.showOpenDialog(mainWindow, {
    title: 'Select folder with EPUB files',
    properties: ['openDirectory'],
  });
  if (canceled) return [];
  return findEpubs(filePaths[0]);
});

ipcMain.handle('dialog:openOutput', async () => {
  const { canceled, filePaths } = await dialog.showOpenDialog(mainWindow, {
    title: 'Select output directory',
    properties: ['openDirectory', 'createDirectory'],
  });
  return canceled ? null : filePaths[0];
});

// ---------------------------------------------------------------------------
// IPC – resolve drag-and-drop paths (files or folders)
// ---------------------------------------------------------------------------

ipcMain.handle('resolve-paths', async (event, paths) => {
  const results = [];
  for (const p of paths) {
    try {
      const stat = fs.statSync(p);
      if (stat.isDirectory()) results.push(...findEpubs(p));
      else if (SUPPORTED_EXT.has(path.extname(p).toLowerCase())) results.push(p);
    } catch { /* skip */ }
  }
  return results;
});

// ---------------------------------------------------------------------------
// Language → locale prefix mapping
// (langdetect returns ISO 639-1 codes; edge-tts uses BCP-47 locale prefixes)
// ---------------------------------------------------------------------------

const LANG_LOCALE_PREFIX = {
  af: 'af-ZA', ar: 'ar-',  bg: 'bg-BG', bn: 'bn-',  ca: 'ca-ES',
  cs: 'cs-CZ', cy: 'cy-GB', da: 'da-DK', de: 'de-',  el: 'el-GR',
  en: 'en-',   es: 'es-',  et: 'et-EE', fa: 'fa-IR', fi: 'fi-FI',
  fr: 'fr-',   ga: 'ga-IE', gl: 'gl-ES', gu: 'gu-IN', hi: 'hi-IN',
  hr: 'hr-HR', hu: 'hu-HU', hy: 'hy-AM', id: 'id-ID', is: 'is-IS',
  it: 'it-IT', ja: 'ja-JP', ka: 'ka-GE', kk: 'kk-KZ', km: 'km-KH',
  ko: 'ko-KR', lt: 'lt-LT', lv: 'lv-LV', mk: 'mk-MK', ml: 'ml-IN',
  mn: 'mn-MN', mr: 'mr-IN', ms: 'ms-MY', mt: 'mt-MT', my: 'my-MM',
  nb: 'nb-NO', nl: 'nl-',  pl: 'pl-PL', ps: 'ps-AF', pt: 'pt-',
  ro: 'ro-RO', ru: 'ru-RU', si: 'si-LK', sk: 'sk-SK', sl: 'sl-SI',
  so: 'so-SO', sq: 'sq-AL', sr: 'sr-RS', sv: 'sv-SE', sw: 'sw-',
  ta: 'ta-IN', te: 'te-IN', th: 'th-TH', tr: 'tr-TR', uk: 'uk-UA',
  ur: 'ur-PK', uz: 'uz-UZ', vi: 'vi-VN', zh: 'zh-',  zu: 'zu-ZA',
};

// Human-readable language names for the UI
const LANG_NAMES = {
  af: 'Afrikaans', ar: 'Arabisch',  bg: 'Bulgarisch', bn: 'Bengalisch',
  ca: 'Katalanisch', cs: 'Tschechisch', cy: 'Walisisch', da: 'Dänisch',
  de: 'Deutsch',  el: 'Griechisch', en: 'Englisch',  es: 'Spanisch',
  et: 'Estnisch', fa: 'Persisch',   fi: 'Finnisch',  fr: 'Französisch',
  ga: 'Irisch',   gl: 'Galizisch',  gu: 'Gujarati',  hi: 'Hindi',
  hr: 'Kroatisch', hu: 'Ungarisch', hy: 'Armenisch', id: 'Indonesisch',
  is: 'Isländisch', it: 'Italienisch', ja: 'Japanisch', ka: 'Georgisch',
  kk: 'Kasachisch', km: 'Khmer',   ko: 'Koreanisch', lt: 'Litauisch',
  lv: 'Lettisch', mk: 'Mazedonisch', ml: 'Malayalam', mn: 'Mongolisch',
  mr: 'Marathi',  ms: 'Malaiisch', mt: 'Maltesisch', my: 'Birmanisch',
  nb: 'Norwegisch', nl: 'Niederländisch', pl: 'Polnisch', pt: 'Portugiesisch',
  ro: 'Rumänisch', ru: 'Russisch', sk: 'Slowakisch', sl: 'Slowenisch',
  sq: 'Albanisch', sr: 'Serbisch', sv: 'Schwedisch', sw: 'Suaheli',
  ta: 'Tamilisch', te: 'Telugu',   th: 'Thailändisch', tr: 'Türkisch',
  uk: 'Ukrainisch', ur: 'Urdu',    uz: 'Usbekisch',  vi: 'Vietnamesisch',
  zh: 'Chinesisch', zu: 'Zulu',
};

// ---------------------------------------------------------------------------
// IPC – python / voices
// ---------------------------------------------------------------------------

ipcMain.handle('get-python-status', async () => {
  const cmd = await getPython();
  return cmd;
});

ipcMain.handle('load-voices', async () => {
  const cmd = await getPython();
  if (!cmd) return { error: 'Python nicht gefunden. Bitte Python 3 installieren und zu PATH hinzufügen.' };

  // Write a tiny temp script – avoids shell newline issues on Windows
  const tmpScript = path.join(app.getPath('temp'), 'etv_list_voices.py');
  fs.writeFileSync(tmpScript, [
    'import asyncio, sys, edge_tts',
    '# On Windows without a console (e.g. spawned from Electron) the default',
    '# ProactorEventLoop can fail; SelectorEventLoop is more robust here.',
    'if sys.platform == "win32":',
    '    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())',
    'async def run():',
    '    vs = await edge_tts.list_voices()',
    '    for v in vs: print(v["ShortName"], v["Locale"], v["Gender"])',
    'asyncio.run(run())',
  ].join('\n'));

  return new Promise(resolve => {
    const proc = spawn(cmd, [tmpScript], {
      shell: false,
      env: { ...process.env, PYTHONIOENCODING: 'utf-8' },
    });
    let out = '', err = '';
    proc.stdout.on('data', d => (out += d));
    proc.stderr.on('data', d => (err += d));
    proc.on('close', code => {
      if (code !== 0) {
        let msg;
        if (err.includes('No module named')) {
          msg = `Abhängigkeiten fehlen. Bitte ausführen:\n  pip install -r requirements.txt`;
        } else if (err.includes('getaddrinfo') || err.includes('ClientConnectorDNSError') || err.includes('Cannot connect to host')) {
          msg = `Edge TTS: Kein Internetzugang oder speech.platform.bing.com nicht erreichbar.\nStimmen können offline nicht geladen werden.\n→ Piper TTS (offline) als Alternative wählen.`;
        } else {
          msg = err.trim() || 'Stimmen konnten nicht geladen werden';
        }
        resolve({ error: msg });
        return;
      }
      const voices = out.trim().split('\n')
        .filter(Boolean)
        .map(line => {
          const parts = line.trim().split(/\s+/);
          return parts.length >= 3 ? { name: parts[0], locale: parts[1], gender: parts[2] } : null;
        })
        .filter(Boolean);
      resolve({ voices });
    });
    proc.on('error', e => resolve({ error: e.message }));
  });
});

// ---------------------------------------------------------------------------
// IPC – document structure (for preview modal)
// ---------------------------------------------------------------------------

ipcMain.handle('get-structure', async (event, filePath) => {
  const cmd = await getPython();
  if (!cmd) return { error: 'Python nicht gefunden' };

  return new Promise(resolve => {
    const proc = spawn(
      cmd,
      [getScriptPath(), filePath, '--get-structure'],
      { shell: false, env: { ...process.env, PYTHONIOENCODING: 'utf-8' } }
    );
    let out = '', err = '';
    proc.stdout.on('data', d => (out += d));
    proc.stderr.on('data', d => (err += d));
    proc.on('close', () => {
      try {
        resolve(JSON.parse(out.trim()));
      } catch {
        resolve({ error: err || 'Ungültige Antwort' });
      }
    });
    proc.on('error', e => resolve({ error: e.message }));
  });
});

// ---------------------------------------------------------------------------
// IPC – language detection
// ---------------------------------------------------------------------------

ipcMain.handle('detect-language', async (event, filePath) => {
  const cmd = await getPython();
  if (!cmd) return { error: 'Python nicht gefunden' };

  return new Promise(resolve => {
    const proc = spawn(
      cmd,
      [getScriptPath(), filePath, '--detect-language'],
      { shell: false, env: { ...process.env, PYTHONIOENCODING: 'utf-8' } }
    );
    let out = '', err = '';
    proc.stdout.on('data', d => (out += d));
    proc.stderr.on('data', d => (err += d));
    proc.on('close', () => {
      try {
        const data  = JSON.parse(out.trim());
        const lang  = data.language;
        if (!lang) { resolve({ error: data.error || 'Sprache nicht erkannt' }); return; }

        const prefix    = LANG_LOCALE_PREFIX[lang] || '';
        const langName  = LANG_NAMES[lang] || lang.toUpperCase();
        resolve({ language: lang, langName, localePrefix: prefix, confidence: data.confidence, method: data.method });
      } catch {
        resolve({ error: 'Ungültige Antwort: ' + out.slice(0, 100) });
      }
    });
    proc.on('error', e => resolve({ error: e.message }));
  });
});

// ---------------------------------------------------------------------------
// IPC – conversion
// ---------------------------------------------------------------------------

const activeJobs = new Map(); // jobId → ChildProcess

ipcMain.handle('demo-voice', async (event, { voice, rate, volume }) => {
  const cmd = await getPython();
  if (!cmd) return { error: 'Python nicht gefunden' };

  const demoScript = path.join(app.getPath('temp'), 'etv_demo.py');
  const demoMp3    = path.join(app.getPath('temp'), 'etv_demo.mp3');

  fs.writeFileSync(demoScript, [
    'import asyncio, edge_tts, sys',
    'if sys.platform == "win32":',
    '    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())',
    'TEXT = "Willkommen! Dies ist eine Hörprobe deiner gewählten Stimme. Ich lese Bücher vor – klar, natürlich und angenehm."',
    'async def run():',
    '    voice, rate, volume = sys.argv[1], sys.argv[2], sys.argv[3]',
    '    comm = edge_tts.Communicate(TEXT, voice, rate=rate, volume=volume)',
    '    await comm.save(sys.argv[4])',
    'asyncio.run(run())',
  ].join('\n'));

  return new Promise(resolve => {
    const proc = spawn(cmd, [demoScript, voice, rate, volume, demoMp3], {
      shell: false,
      env: { ...process.env, PYTHONIOENCODING: 'utf-8' },
    });
    let err = '';
    proc.stderr.on('data', d => (err += d));
    proc.on('close', code => {
      if (code !== 0) { resolve({ error: err || 'Demo fehlgeschlagen' }); return; }
      try {
        const b64 = fs.readFileSync(demoMp3).toString('base64');
        resolve({ base64: b64 });
      } catch (e) {
        resolve({ error: e.message });
      }
    });
    proc.on('error', e => resolve({ error: e.message }));
  });
});

ipcMain.handle('start-conversion', async (event, opts) => {
  const { jobId, epubPath, outputDir, voice, rate, volume, skipShort, maxChapters, merge, createZip,
          startPage, endPage, skipChapters, translateTo, resume, ttsEngine, piperVoice } = opts;

  const cmd = await getPython();
  if (!cmd) return { error: 'Python not found' };

  const args = [getScriptPath(), epubPath];
  if (outputDir)               args.push('-o', outputDir);
  if (voice)                   args.push('-v', voice);
  if (rate)                    args.push(`--rate=${rate}`);
  if (volume)                  args.push(`--volume=${volume}`);
  if (skipShort != null)       args.push(`--skip-short=${skipShort}`);
  if (maxChapters != null)     args.push(`--max-chapters=${maxChapters}`);
  if (merge)                   args.push('--merge');
  if (startPage != null)       args.push(`--start-page=${startPage}`);
  if (endPage   != null)       args.push(`--end-page=${endPage}`);
  if (skipChapters && skipChapters.length) args.push(`--skip-chapters=${skipChapters.join(',')}`);
  if (translateTo)                         args.push(`--translate-to=${translateTo}`);
  if (resume)                              args.push('--resume');
  if (ttsEngine && ttsEngine !== 'edge')   args.push(`--tts-engine=${ttsEngine}`);
  if (piperVoice)                          args.push(`--piper-voice=${piperVoice}`);

  return new Promise(resolve => {
    const proc = spawn(cmd, args, { shell: false, env: { ...process.env, PYTHONIOENCODING: 'utf-8' } });
    activeJobs.set(jobId, proc);

    let outputPath  = null;
    let totalChapters = null;
    let buf = '';

    function flush(line) {
      if (!line.trim()) return;

      // Extract output path
      const m1 = line.match(/Output:\s+(.+)/);
      if (m1) outputPath = m1[1].trim();

      // Extract total chapters
      const m2 = line.match(/Chapters:\s+(\d+)/);
      if (m2) totalChapters = parseInt(m2[1]);

      // Extract per-chapter progress
      const m3 = line.match(/\[\s*(\d+)\s*\/\s*(\d+)\]/);
      const progress = m3 ? { current: parseInt(m3[1]), total: parseInt(m3[2]) } : null;

      event.sender.send('conversion-progress', { jobId, line, progress });
    }

    proc.stdout.on('data', chunk => {
      buf += chunk.toString();
      const parts = buf.split('\n');
      buf = parts.pop();
      parts.forEach(flush);
      // Also emit the incomplete line if it looks like a progress line
      if (buf.includes('chars) …')) { flush(buf); buf = ''; }
    });

    let stderrBuf = '';
    proc.stderr.on('data', chunk => {
      const text = chunk.toString();
      stderrBuf += text;
      event.sender.send('conversion-progress', { jobId, line: text.trim(), isError: true });
    });

    proc.on('close', async code => {
      if (buf.trim()) flush(buf);
      activeJobs.delete(jobId);

      if (code === 0 && createZip && outputPath) {
        try {
          event.sender.send('conversion-progress', { jobId, line: '📦  Creating ZIP archive…' });
          const zipPath = outputPath.replace(/[/\\]$/, '') + '.zip';
          await zipFolder(outputPath, zipPath);
          event.sender.send('conversion-progress', { jobId, line: `✅  ZIP saved: ${zipPath}` });
          resolve({ success: true, outputPath: zipPath });
        } catch (e) {
          resolve({ success: false, error: 'ZIP failed: ' + e.message });
        }
      } else if (code === 0) {
        resolve({ success: true, outputPath });
      } else {
        let errMsg = `Python exited with code ${code}`;
        if (stderrBuf.includes('piper-tts') || stderrBuf.includes('No module named')) {
          errMsg = stderrBuf.trim();
        }
        resolve({ success: false, error: errMsg, exitCode: code });
      }
    });

    proc.on('error', e => {
      activeJobs.delete(jobId);
      resolve({ success: false, error: e.message });
    });
  });
});

ipcMain.handle('cancel-job', (event, jobId) => {
  const proc = activeJobs.get(jobId);
  if (!proc) return false;
  proc.kill();
  activeJobs.delete(jobId);
  return true;
});

ipcMain.handle('reveal-path', (event, p) => {
  if (p.startsWith('http://') || p.startsWith('https://')) {
    shell.openExternal(p);
  } else {
    shell.showItemInFolder(p);
  }
});

ipcMain.handle('check-resumable', (event, filePath, customOutputDir) => {
  const parsed = path.parse(filePath);
  const stem   = parsed.name;
  const outDir = customOutputDir
    ? path.join(customOutputDir, stem)
    : path.join(parsed.dir, stem);
  try {
    const files  = fs.readdirSync(outDir);
    const audio  = files.filter(f => /\.(mp3|wav)$/i.test(f));
    return { resumable: audio.length > 0, outputDir: outDir, mp3Count: audio.length };
  } catch {
    return { resumable: false };
  }
});

// ---------------------------------------------------------------------------
// ZIP helper
// ---------------------------------------------------------------------------

function zipFolder(srcFolder, destZip) {
  return new Promise((resolve, reject) => {
    const out = fs.createWriteStream(destZip);
    const arc = archiver('zip', { zlib: { level: 6 } });
    out.on('close', resolve);
    arc.on('error', reject);
    arc.pipe(out);
    arc.directory(srcFolder, path.basename(srcFolder));
    arc.finalize();
  });
}
