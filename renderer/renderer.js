'use strict';

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------
let queue       = [];     // { id, path, name, status, current, total, outputPath, subText }
let isConverting = false;
let stopRequested = false;
let currentJobId  = null;
let unsubscribeProgress = null;
let allVoices   = [];     // full list loaded at startup (for language-based filtering)

const settings = {
  outputDir: '',
  merge:     false,
  createZip: false,
  voice:     'de-DE-ConradNeural',
  rate:      '-10%',
  volume:    '+0%',
  skipShort: 60,
};

// ---------------------------------------------------------------------------
// DOM refs
// ---------------------------------------------------------------------------
const $ = id => document.getElementById(id);

const dropZone     = $('dropZone');
const queueList    = $('queueList');
const queueEmpty   = $('queueEmpty');
const queueCount   = $('queueCount');
const logOutput    = $('logOutput');
const logDetails   = $('logDetails');
const pythonBadge  = $('pythonBadge');
const voiceStatus   = $('voiceStatus');
const voiceRow      = $('voiceRow');
const voiceInput    = $('voiceInput');
const voiceDatalist = $('voiceDatalist');
const demoStatus    = $('demoStatus');
const langSuggest   = $('langSuggest');

const rateSlider   = $('rateSlider');
const rateDisplay  = $('rateDisplay');
const volumeSlider = $('volumeSlider');
const volumeDisplay= $('volumeDisplay');
const skipShortIn  = $('skipShort');
const outputDirIn  = $('outputDir');
const chkZip       = $('chkZip');
const btnStart     = $('btnStart');
const btnPreview   = $('btnPreview');
const btnCancel    = $('btnCancel');
const btnClear     = $('btnClearQueue');

// ---------------------------------------------------------------------------
// Initialisation
// ---------------------------------------------------------------------------
async function init() {
  checkPython();
  loadVoices();
  bindEvents();
}

async function checkPython() {
  const cmd = await window.api.getPythonStatus();
  if (cmd) {
    pythonBadge.textContent = `Python: ${cmd}`;
    pythonBadge.className   = 'badge badge--ok';
  } else {
    pythonBadge.textContent = 'Python nicht gefunden!';
    pythonBadge.className   = 'badge badge--error';
    pythonBadge.title = 'Bitte Python 3 installieren und zu PATH hinzufügen, dann neu starten.';
  }
}

async function loadVoices() {
  const result = await window.api.loadVoices();
  voiceStatus.style.display = 'none';
  voiceInput.style.display  = '';

  if (result.error) {
    voiceStatus.style.display    = '';
    voiceRow.style.display       = 'none';
    voiceStatus.style.color      = 'var(--error)';
    voiceStatus.style.whiteSpace = 'pre-line';
    voiceStatus.textContent      = result.error;
    return;
  }

  voiceStatus.style.display = 'none';
  voiceRow.style.display    = '';

  allVoices = result.voices;
  for (const v of result.voices) {
    const opt = document.createElement('option');
    opt.value = v.name;
    opt.label = `${v.name}  (${v.locale} · ${v.gender})`;
    voiceDatalist.appendChild(opt);
  }
  voiceInput.value = settings.voice;
}

// ---------------------------------------------------------------------------
// Event bindings
// ---------------------------------------------------------------------------
function bindEvents() {
  // Drop zone
  dropZone.addEventListener('dragover', e => {
    e.preventDefault();
    e.dataTransfer.dropEffect = 'copy';
    dropZone.classList.add('drag-over');
  });
  dropZone.addEventListener('dragleave', e => {
    if (!dropZone.contains(e.relatedTarget)) dropZone.classList.remove('drag-over');
  });
  dropZone.addEventListener('drop', async e => {
    e.preventDefault();
    dropZone.classList.remove('drag-over');
    const files = [...e.dataTransfer.files];
    if (!files.length) return;
    const paths = files.map(f => window.api.getFilePath(f)).filter(Boolean);
    const epubs = await window.api.resolvePaths(paths);
    addToQueue(epubs);
  });

  // Buttons
  $('btnAddFiles').addEventListener('click', async () => {
    const files = await window.api.openFiles();
    addToQueue(files);
  });
  $('btnAddFolder').addEventListener('click', async () => {
    const files = await window.api.openFolder();
    addToQueue(files);
  });
  $('btnBrowseOutput').addEventListener('click', async () => {
    const dir = await window.api.openOutput();
    if (dir) { outputDirIn.value = dir; settings.outputDir = dir; }
  });

  outputDirIn.addEventListener('input',  () => { settings.outputDir = outputDirIn.value.trim(); });
  $('chkMerge').addEventListener('change', () => { settings.merge = $('chkMerge').checked; });
  chkZip.addEventListener('change',        () => { settings.createZip = chkZip.checked; });
  voiceInput.addEventListener('change',  () => { settings.voice = voiceInput.value.trim(); });
  voiceInput.addEventListener('input',   () => { settings.voice = voiceInput.value.trim(); });

  rateSlider.addEventListener('input', () => {
    const v = parseInt(rateSlider.value);
    settings.rate   = (v >= 0 ? '+' : '') + v + '%';
    rateDisplay.textContent = settings.rate;
  });
  volumeSlider.addEventListener('input', () => {
    const v = parseInt(volumeSlider.value);
    settings.volume = (v >= 0 ? '+' : '') + v + '%';
    volumeDisplay.textContent = settings.volume;
  });
  skipShortIn.addEventListener('input', () => {
    settings.skipShort = parseInt(skipShortIn.value) || 0;
  });

  btnStart.addEventListener('click',   () => startConversion(false));
  btnPreview.addEventListener('click', () => startConversion(true));
  btnCancel.addEventListener('click',  cancelConversion);
  btnClear.addEventListener('click',   () => {
    if (!isConverting) { queue = []; renderQueue(); }
  });

  // Voice demo
  let demoAudio = null;
  $('btnDemoVoice').addEventListener('click', async () => {
    const voice = voiceInput.value.trim();
    if (!voice) return;

    if (demoAudio) { demoAudio.pause(); demoAudio = null; }

    demoStatus.textContent = '⏳ Stimme wird generiert…';
    demoStatus.className   = 'demo-status loading';
    $('btnDemoVoice').disabled = true;

    const result = await window.api.demoVoice(voice, settings.rate, settings.volume);
    $('btnDemoVoice').disabled = false;

    if (result.error) {
      demoStatus.textContent = '✗ ' + result.error;
      demoStatus.className   = 'demo-status error';
      return;
    }

    demoStatus.textContent = '▶ Spielt ab…';
    demoStatus.className   = 'demo-status';
    demoAudio = new Audio(`data:audio/mpeg;base64,${result.base64}`);
    demoAudio.play();
    demoAudio.onended = () => { demoStatus.textContent = ''; demoAudio = null; };
  });

  // Voice settings toggle
  const toggleBtn   = $('toggleVoice');
  const voicePanel  = $('voiceSettings');
  toggleBtn.addEventListener('click', () => {
    const open = voicePanel.classList.toggle('open');
    toggleBtn.setAttribute('aria-expanded', open);
  });
}

// ---------------------------------------------------------------------------
// Queue management
// ---------------------------------------------------------------------------
function addToQueue(epubPaths) {
  if (!epubPaths || !epubPaths.length) return;
  const existing = new Set(queue.map(j => j.path));
  const newPaths = [];
  for (const p of epubPaths) {
    if (existing.has(p)) continue;
    queue.push({ id: crypto.randomUUID(), path: p, name: baseName(p), status: 'queued', current: 0, total: 0, outputPath: null, subText: '' });
    newPaths.push(p);
  }
  if (newPaths.length) {
    log(`${newPaths.length} Datei(en) zur Warteschlange hinzugefügt.`);
    // Detect language from the first newly added file
    detectAndSuggestVoice(newPaths[0]);
  }
  renderQueue();
}

// ---------------------------------------------------------------------------
// Language detection & voice suggestion
// ---------------------------------------------------------------------------
async function detectAndSuggestVoice(filePath) {
  langSuggest.style.display = 'none';
  langSuggest.innerHTML     = '';

  const result = await window.api.detectLanguage(filePath);
  if (result.error || !result.language) return;

  // Filter loaded voices by locale prefix (up to 4)
  const prefix    = result.localePrefix || '';
  const matching  = allVoices.filter(v => v.locale.startsWith(prefix)).slice(0, 4);
  if (!matching.length) return;

  // Skip suggestion if current voice already matches the detected language
  if (voiceInput.value && voiceInput.value.startsWith(prefix.replace(/-$/, ''))) return;

  // Build suggestion banner
  const conf     = result.confidence >= 0.7 ? '' : ' <span class="lang-conf">(unsicher)</span>';
  const pills    = matching.map(v =>
    `<button class="lang-pill" data-voice="${escHtml(v.name)}" title="${escHtml(v.locale)} · ${escHtml(v.gender)}">${escHtml(v.name)}</button>`
  ).join('');

  langSuggest.innerHTML =
    `<span class="lang-flag">🌍</span>` +
    `<span class="lang-label">Erkannte Sprache: <strong>${escHtml(result.langName)}</strong>${conf}</span>` +
    `<span class="lang-voices">${pills}</span>` +
    `<button class="lang-dismiss" title="Schließen">✕</button>`;

  langSuggest.style.display = 'flex';

  // Wire up pill clicks → set voice
  langSuggest.querySelectorAll('.lang-pill').forEach(btn => {
    btn.addEventListener('click', () => {
      const voice = btn.dataset.voice;
      voiceInput.value = voice;
      settings.voice   = voice;
      langSuggest.style.display = 'none';
      log(`🌍 Stimme gewechselt zu: ${voice}`);
    });
  });

  langSuggest.querySelector('.lang-dismiss').addEventListener('click', () => {
    langSuggest.style.display = 'none';
  });
}

function renderQueue() {
  // Remove old dynamic items
  [...queueList.querySelectorAll('.queue-item')].forEach(el => el.remove());

  queueEmpty.style.display = queue.length ? 'none' : '';
  queueCount.textContent   = queue.length;
  const noQueue = queue.length === 0 || isConverting;
  btnStart.disabled   = noQueue;
  btnPreview.disabled = noQueue;

  for (const job of queue) {
    queueList.insertBefore(buildJobEl(job), queueEmpty);
  }
}

function buildJobEl(job) {
  const el = document.createElement('div');
  el.className = `queue-item queue-item--${job.status}`;
  el.dataset.jobId = job.id;

  const pct = job.total > 0 ? Math.round((job.current / job.total) * 100) : 0;
  const statusLabels = { queued: 'Wartend', processing: 'Läuft…', done: 'Fertig', error: 'Fehler', cancelled: 'Abgebrochen' };

  el.innerHTML = `
    <div class="item-top">
      <span class="item-name" title="${escHtml(job.path)}">${escHtml(job.name)}</span>
      <span class="status status--${job.status}">${statusLabels[job.status] ?? job.status}</span>
      ${job.status === 'queued' ? `<button class="item-remove" data-remove="${job.id}" title="Entfernen">✕</button>` : ''}
    </div>
    <div class="progress-wrap">
      <div class="progress-bar" style="width:${pct}%"></div>
    </div>
    <div class="item-sub">${escHtml(job.subText || (job.status === 'queued' ? job.path : ''))}</div>
    ${job.outputPath ? `
    <div class="item-actions">
      <button class="btn btn--secondary btn--sm" data-reveal="${escHtml(job.outputPath)}">📂 Öffnen</button>
    </div>` : ''}
  `;

  // Wire up buttons inside the item
  el.querySelector('[data-remove]')?.addEventListener('click', e => {
    const id = e.currentTarget.dataset.remove;
    queue = queue.filter(j => j.id !== id);
    renderQueue();
  });
  el.querySelector('[data-reveal]')?.addEventListener('click', e => {
    window.api.revealPath(e.currentTarget.dataset.reveal);
  });

  return el;
}

function updateJobEl(job) {
  const el = queueList.querySelector(`[data-job-id="${job.id}"]`);
  if (!el) return;
  // Rebuild the element in place
  const newEl = buildJobEl(job);
  queueList.replaceChild(newEl, el);
}

function getJob(id) { return queue.find(j => j.id === id); }

// ---------------------------------------------------------------------------
// Conversion
// ---------------------------------------------------------------------------
async function startConversion(previewMode = false) {
  if (isConverting || !queue.length) return;

  isConverting  = true;
  stopRequested = false;
  btnStart.style.display   = 'none';
  btnPreview.style.display = 'none';
  btnCancel.style.display  = '';

  // Subscribe to progress events once
  unsubscribeProgress = window.api.onProgress(handleProgress);

  const pending = queue.filter(j => j.status === 'queued');
  log(previewMode
    ? `🎧 Hörprobe (max. 3 Kapitel): ${pending.length} Datei(en)…`
    : `Starte Konvertierung: ${pending.length} Datei(en)…`);

  for (const job of pending) {
    if (stopRequested) break;

    currentJobId = job.id;
    job.status   = 'processing';
    job.subText  = 'Verbinde mit Edge TTS…';
    renderQueue();
    updateJobEl(job);

    const opts = {
      jobId:       job.id,
      epubPath:    job.path,
      outputDir:   settings.outputDir || null,
      voice:       settings.voice,
      rate:        settings.rate,
      volume:      settings.volume,
      skipShort:   settings.skipShort,
      maxChapters: previewMode ? 3 : null,
      merge:       settings.merge,
      createZip:   settings.createZip,
    };

    const result = await window.api.startConversion(opts);
    const j = getJob(job.id);
    if (!j) continue; // removed during run

    if (stopRequested) {
      // cancel already set status to 'cancelled' or will shortly
      if (j.status !== 'cancelled') { j.status = 'cancelled'; j.subText = 'Abgebrochen'; }
    } else if (result.success) {
      j.status     = 'done';
      j.outputPath = result.outputPath;
      j.subText    = result.outputPath ? `→ ${result.outputPath}` : 'Fertig';
      log(`✅ Fertig: ${j.name}`, 'ok');
    } else {
      j.status  = 'error';
      j.subText = result.error || 'Unbekannter Fehler';
      log(`✗ Fehler bei ${j.name}: ${j.subText}`, 'error');
    }
    renderQueue();
    updateJobEl(j);
  }

  // Mark remaining queued items as such (no change if not stopped)
  if (stopRequested) {
    const j = getJob(currentJobId);
    if (j && j.status === 'processing') {
      j.status  = 'cancelled';
      j.subText = 'Abgebrochen';
      renderQueue();
    }
    log('Abgebrochen durch Nutzer.', 'warn');
  }

  if (unsubscribeProgress) { unsubscribeProgress(); unsubscribeProgress = null; }
  isConverting    = false;
  stopRequested   = false;
  currentJobId    = null;
  btnStart.style.display   = '';
  btnPreview.style.display = '';
  btnCancel.style.display  = 'none';
  const hasQueued = queue.filter(j => j.status === 'queued').length > 0;
  btnStart.disabled   = !hasQueued;
  btnPreview.disabled = !hasQueued;
}

async function cancelConversion() {
  if (!isConverting) return;
  stopRequested = true;
  if (currentJobId) await window.api.cancelJob(currentJobId);
  const j = getJob(currentJobId);
  if (j) { j.status = 'cancelled'; j.subText = 'Wird abgebrochen…'; updateJobEl(j); }
}

function handleProgress(data) {
  const { jobId, line, progress, isError } = data;
  const j = getJob(jobId);

  if (line) {
    log(line, isError ? 'error' : line.includes('✓') || line.includes('✅') ? 'ok' : null);
  }

  if (!j) return;

  if (progress) {
    j.current = progress.current;
    j.total   = progress.total;
  }

  // Update subtitle to current chapter line
  if (line && line.includes('/') && line.includes('chars)')) {
    j.subText = line.replace(/^\s+/, '').split('…')[0].trim();
  } else if (line && line.includes('Output:')) {
    const m = line.match(/Output:\s+(.+)/);
    if (m) j.outputPath = m[1].trim();
  }

  updateJobEl(j);
}

// ---------------------------------------------------------------------------
// Log
// ---------------------------------------------------------------------------
function log(text, type = null) {
  if (!text || !text.trim()) return;
  const div = document.createElement('div');
  div.className = 'log-line' + (type === 'error' ? ' log-line--error' : type === 'ok' ? ' log-line--ok' : '');
  div.textContent = text.trim();
  logOutput.appendChild(div);
  logOutput.scrollTop = logOutput.scrollHeight;
  if (!logDetails.open) logDetails.open = true;
}

// ---------------------------------------------------------------------------
// Utils
// ---------------------------------------------------------------------------
function baseName(p) {
  return p.replace(/\\/g, '/').split('/').pop();
}
function escHtml(s) {
  if (!s) return '';
  return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

// ---------------------------------------------------------------------------
// Boot
// ---------------------------------------------------------------------------
init();
