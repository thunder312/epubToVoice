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
  outputDir:   '',
  merge:       false,
  createZip:   false,
  voice:       'de-DE-ConradNeural',
  rate:        '-10%',
  volume:      '+0%',
  skipShort:   60,
  translateTo: null,   // e.g. 'de' when translation is enabled
  ttsEngine:   'edge', // 'edge' or 'piper'
  piperVoice:  'de_DE-thorsten-high',
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
const voiceCount    = $('voiceCount');

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
  // Reset state
  voiceStatus.style.display = '';
  voiceStatus.style.color   = '';
  voiceStatus.style.whiteSpace = '';
  voiceStatus.textContent   = 'Stimmen werden geladen…';
  voiceRow.style.display    = 'none';
  voiceCount.style.display  = 'none';

  const result = await window.api.loadVoices();

  if (result.error) {
    const isOffline = result.error.includes('Internetzugang') || result.error.includes('getaddrinfo');
    voiceStatus.style.color      = isOffline ? 'var(--warn, #e6a817)' : 'var(--error)';
    voiceStatus.style.whiteSpace = 'pre-line';
    // Add retry link at the end
    voiceStatus.textContent = result.error;
    const retryBtn = document.createElement('button');
    retryBtn.textContent = '↺ Erneut versuchen';
    retryBtn.className   = 'btn btn--ghost btn--sm';
    retryBtn.style.marginTop = '6px';
    retryBtn.style.display   = 'block';
    retryBtn.addEventListener('click', () => {
      voiceDatalist.innerHTML = '';
      allVoices = [];
      loadVoices();
    });
    voiceStatus.appendChild(retryBtn);
    return;
  }

  voiceStatus.style.display = 'none';
  voiceRow.style.display    = '';

  allVoices = result.voices;
  voiceDatalist.innerHTML = '';
  for (const v of result.voices) {
    const opt = document.createElement('option');
    opt.value = v.name;
    opt.label = `${v.name}  (${v.locale} · ${v.gender})`;
    voiceDatalist.appendChild(opt);
  }
  voiceInput.value = settings.voice;

  voiceCount.textContent   = `${result.voices.length} Stimmen verfügbar – tippen zum Filtern`;
  voiceCount.style.display = '';
  voiceInput.addEventListener('focus', () => voiceInput.select());
  voiceInput.addEventListener('click', () => voiceInput.select());
}

// ---------------------------------------------------------------------------
// Preview modal
// ---------------------------------------------------------------------------
const previewModal  = $('previewModal');
const modalTitle    = $('modalTitle');
const modalBody     = $('modalBody');
const modalClose    = $('modalClose');
const modalCancel   = $('modalCancel');
const modalApply    = $('modalApply');

// Temporary state while modal is open
let _previewJob     = null;   // queue item being previewed
let _previewPending = {};     // {startPage, endPage, skipChapters:[]}

function openPreview(jobId) {
  const job = getJob(jobId);
  if (!job) return;
  _previewJob     = job;
  _previewPending = {
    startPage:    job.startPage    ?? null,
    endPage:      job.endPage      ?? null,
    skipChapters: [...(job.skipChapters ?? [])],
  };

  modalTitle.textContent = job.name;
  modalBody.innerHTML    = '<div class="modal-loading">⏳ Vorschau wird geladen…</div>';
  previewModal.style.display = 'flex';

  // Use cached structure if available, else fetch
  if (job.structure) {
    renderPreviewBody(job.structure);
  } else {
    window.api.getStructure(job.path).then(data => {
      job.structure = data;
      renderPreviewBody(data);
    });
  }
}

function closePreview() {
  previewModal.style.display = 'none';
  _previewJob = null;
}

function renderPreviewBody(data) {
  if (data.error) {
    modalBody.innerHTML = `<p class="modal-error">⚠ ${escHtml(data.error)}</p>`;
    return;
  }
  if (data.type === 'pdf')  renderPdfPreview(data);
  else if (data.type === 'epub') renderEpubPreview(data);
  else if (data.type === 'txt')  renderListPreview(data.sections, 'Abschnitt');
  else modalBody.innerHTML = '<p class="modal-error">Unbekanntes Format</p>';
}

function renderPdfPreview(data) {
  const start = _previewPending.startPage ?? 1;
  const end   = _previewPending.endPage   ?? data.totalPages;
  const more  = data.totalPages > data.thumbCount
    ? `<p class="thumb-note">Vorschau: erste ${data.thumbCount} von ${data.totalPages} Seiten. Seitenbereich per Eingabe anpassen.</p>`
    : '';

  modalBody.innerHTML = `
    <div class="pdf-range">
      <label>Von Seite
        <input type="number" id="rangeStart" class="input input--narrow"
               min="1" max="${data.totalPages}" value="${start}">
      </label>
      <span class="range-sep">–</span>
      <label>Bis Seite
        <input type="number" id="rangeEnd" class="input input--narrow"
               min="1" max="${data.totalPages}" value="${end}">
      </label>
      <span class="range-total">von ${data.totalPages} Seiten</span>
    </div>
    ${more}
    <div class="thumb-strip" id="thumbStrip"></div>
  `;

  const strip = $('thumbStrip');
  data.pages.forEach(p => {
    const inRange = p.page >= start && p.page <= end;
    const badge   = p.looksLikeCover ? '🖼 Cover'
                  : p.looksLikeToc   ? '📋 Inhalt'
                  : '';
    const div = document.createElement('div');
    div.className  = `thumb-item${inRange ? ' thumb-in' : ' thumb-out'}`;
    div.dataset.page = p.page;
    div.innerHTML  = `
      <img src="data:image/png;base64,${p.thumbnail}" class="thumb-img" alt="Seite ${p.page}">
      ${badge ? `<span class="thumb-badge">${badge}</span>` : ''}
      <span class="thumb-num">S.${p.page}</span>
    `;
    div.addEventListener('click', () => toggleThumb(div, p.page, data.totalPages));
    div.addEventListener('mouseenter', e => showThumbTooltip(e, p));
    div.addEventListener('mousemove',  e => moveThumbTooltip(e));
    div.addEventListener('mouseleave', () => hideThumbTooltip());
    strip.appendChild(div);
  });

  // Sync inputs with thumbnail highlight
  const syncHighlight = () => {
    const s = parseInt($('rangeStart').value) || 1;
    const e = parseInt($('rangeEnd').value)   || data.totalPages;
    _previewPending.startPage = s;
    _previewPending.endPage   = e;
    strip.querySelectorAll('.thumb-item').forEach(el => {
      const pg = parseInt(el.dataset.page);
      el.classList.toggle('thumb-in',  pg >= s && pg <= e);
      el.classList.toggle('thumb-out', pg < s  || pg > e);
    });
  };
  $('rangeStart').addEventListener('input', syncHighlight);
  $('rangeEnd').addEventListener('input',   syncHighlight);
}

// Floating tooltip for page thumbnails
let _tooltip = null;
function _ensureTooltip() {
  if (!_tooltip) {
    _tooltip = document.createElement('div');
    _tooltip.className = 'thumb-tooltip';
    document.body.appendChild(_tooltip);
  }
  return _tooltip;
}
function showThumbTooltip(e, p) {
  const tip  = _ensureTooltip();
  const flag = p.looksLikeCover ? '🖼 Cover · ' : p.looksLikeToc ? '📋 Inhaltsverzeichnis · ' : '';
  const text = (p.textPreview || '').trim();
  // Format text as short paragraphs (split on long runs)
  const lines = text.match(/.{1,60}(\s|$)/g) || [text];
  tip.innerHTML =
    `<div class="tip-header">${flag}Seite ${p.page}</div>` +
    `<div class="tip-body">${escHtml(lines.slice(0, 6).join('\n'))}</div>`;
  tip.style.display = 'block';
  moveThumbTooltip(e);
}
function moveThumbTooltip(e) {
  if (!_tooltip) return;
  const x = e.clientX + 14;
  const y = e.clientY + 14;
  // Keep inside viewport
  const tw = _tooltip.offsetWidth || 220;
  const th = _tooltip.offsetHeight || 80;
  _tooltip.style.left = (x + tw > window.innerWidth  ? e.clientX - tw - 8 : x) + 'px';
  _tooltip.style.top  = (y + th > window.innerHeight ? e.clientY - th - 8 : y) + 'px';
}
function hideThumbTooltip() {
  if (_tooltip) _tooltip.style.display = 'none';
}

function toggleThumb(el, page, total) {
  const s = parseInt($('rangeStart')?.value) || 1;
  const e = parseInt($('rangeEnd')?.value)   || total;
  // clicking left half → set start, right half → set end
  if (page <= s || page < (s + e) / 2) {
    $('rangeStart').value = page;
  } else {
    $('rangeEnd').value = page;
  }
  $('rangeStart').dispatchEvent(new Event('input'));
}

function renderEpubPreview(data) {
  renderListPreview(data.chapters, 'Kapitel');
}

function renderListPreview(items, label) {
  const rows = items.map(item => {
    const idx     = item.index;
    const checked = !_previewPending.skipChapters.includes(idx);
    const hint    = item.isToc ? ' <span class="item-hint">Inhalt</span>' : '';
    const kb      = item.chars > 0 ? ` <span class="item-chars">${Math.round(item.chars/1000)}k</span>` : '';
    return `
      <label class="chapter-row${item.isToc ? ' chapter-row--toc' : ''}">
        <input type="checkbox" data-idx="${idx}" ${checked ? 'checked' : ''}>
        <span class="chapter-title">${escHtml(item.title || `${label} ${idx+1}`)}${hint}${kb}</span>
      </label>`;
  }).join('');

  modalBody.innerHTML = `
    <div class="chapter-actions">
      <button class="btn btn--ghost btn--sm" id="chkAll">Alle</button>
      <button class="btn btn--ghost btn--sm" id="chkNone">Keine</button>
      <button class="btn btn--ghost btn--sm" id="chkAuto">Auto (TOC abwählen)</button>
    </div>
    <div class="chapter-list">${rows}</div>`;

  const getChecked = () =>
    [...modalBody.querySelectorAll('input[type=checkbox]')]
      .filter(cb => !cb.checked).map(cb => parseInt(cb.dataset.idx));

  const setAll = val => modalBody.querySelectorAll('input[type=checkbox]').forEach(cb => cb.checked = val);

  $('chkAll').addEventListener('click',  () => setAll(true));
  $('chkNone').addEventListener('click', () => setAll(false));
  $('chkAuto').addEventListener('click', () => {
    modalBody.querySelectorAll('input[type=checkbox]').forEach(cb => {
      const row = cb.closest('.chapter-row');
      cb.checked = !row.classList.contains('chapter-row--toc');
    });
  });

  modalBody.querySelectorAll('input[type=checkbox]').forEach(cb =>
    cb.addEventListener('change', () => { _previewPending.skipChapters = getChecked(); })
  );
  // Init
  _previewPending.skipChapters = getChecked();
}

// ---------------------------------------------------------------------------
// Event bindings
// ---------------------------------------------------------------------------
function bindEvents() {
  // Preview modal buttons
  modalClose.addEventListener('click',  closePreview);
  modalCancel.addEventListener('click', closePreview);
  previewModal.addEventListener('click', e => { if (e.target === previewModal) closePreview(); });
  modalApply.addEventListener('click', () => {
    if (!_previewJob) return;
    // Sync range inputs if PDF
    if ($('rangeStart')) _previewPending.startPage = parseInt($('rangeStart').value) || null;
    if ($('rangeEnd'))   _previewPending.endPage   = parseInt($('rangeEnd').value)   || null;
    _previewJob.startPage    = _previewPending.startPage;
    _previewJob.endPage      = _previewPending.endPage;
    _previewJob.skipChapters = [..._previewPending.skipChapters];
    closePreview();
    renderQueue();
  });

  // Drag & drop – accept anywhere in the window, highlight the drop zone
  document.addEventListener('dragover', e => {
    e.preventDefault();
    e.dataTransfer.dropEffect = 'copy';
    dropZone.classList.add('drag-over');
  });
  document.addEventListener('dragleave', e => {
    // Only remove highlight when leaving the window entirely
    if (!e.relatedTarget) dropZone.classList.remove('drag-over');
  });
  document.addEventListener('drop', async e => {
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

  // Translation
  $('chkTranslate').addEventListener('change', () => {
    const enabled = $('chkTranslate').checked;
    $('translateOptions').style.display = enabled ? '' : 'none';
    if (enabled) {
      settings.translateTo = $('translateTarget').value;
      suggestVoiceForLang(settings.translateTo);
    } else {
      settings.translateTo = null;
    }
  });
  $('translateTarget').addEventListener('change', () => {
    settings.translateTo = $('translateTarget').value;
    suggestVoiceForLang(settings.translateTo);
  });

  // TTS engine toggle
  document.querySelectorAll('input[name="ttsEngine"]').forEach(radio => {
    radio.addEventListener('change', () => {
      settings.ttsEngine = radio.value;
      const isPiper = radio.value === 'piper';
      $('edgeVoicePanel').style.display  = isPiper ? 'none' : '';
      $('piperVoicePanel').style.display = isPiper ? '' : 'none';
    });
  });
  $('piperVoiceInput').addEventListener('input', () => {
    settings.piperVoice = $('piperVoiceInput').value.trim();
  });
  $('piperVoicesLink').addEventListener('click', e => {
    e.preventDefault();
    // Open in external browser via shell
    window.api.revealPath('https://huggingface.co/rhasspy/piper-voices/tree/main');
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

}

// ---------------------------------------------------------------------------
// Queue management
// ---------------------------------------------------------------------------
async function addToQueue(epubPaths) {
  if (!epubPaths || !epubPaths.length) return;
  const existing = new Set(queue.map(j => j.path));
  const newJobs = [];
  for (const p of epubPaths) {
    if (existing.has(p)) continue;
    const job = { id: crypto.randomUUID(), path: p, name: baseName(p), status: 'queued', current: 0, total: 0, outputPath: null, subText: '', startPage: null, endPage: null, skipChapters: [], structure: null, resume: false };
    queue.push(job);
    newJobs.push(job);
  }
  if (newJobs.length) {
    log(`${newJobs.length} Datei(en) zur Warteschlange hinzugefügt.`);
    detectAndSuggestVoice(newJobs[0].path);
    // Check each new job for existing partial output
    for (const job of newJobs) {
      const r = await window.api.checkResumable(job.path, settings.outputDir || null);
      if (r.resumable) {
        job.status  = 'resumable';
        job.resume  = true;
        job.subText = `${r.mp3Count} Split(s) bereits vorhanden – Fortsetzen möglich`;
        log(`▶ ${job.name}: ${r.mp3Count} Split(s) gefunden – "Fortsetzen" klicken um weiterzumachen.`, 'warn');
      }
    }
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

// Locale prefix for a 2-letter language code (matches main.js LANG_LOCALE_PREFIX)
const LANG_LOCALE_PREFIX = {
  de: 'de-', en: 'en-', fr: 'fr-', es: 'es-', it: 'it-',
  nl: 'nl-', pt: 'pt-', ru: 'ru-', zh: 'zh-', ja: 'ja-',
  ko: 'ko-', pl: 'pl-', sv: 'sv-', da: 'da-', fi: 'fi-',
  nb: 'nb-', tr: 'tr-', ar: 'ar-', hi: 'hi-', cs: 'cs-',
};
const LANG_NAMES = {
  de: 'Deutsch', en: 'Englisch', fr: 'Französisch', es: 'Spanisch',
  it: 'Italienisch', nl: 'Niederländisch', pt: 'Portugiesisch',
  ru: 'Russisch', zh: 'Chinesisch', ja: 'Japanisch', ko: 'Koreanisch',
  pl: 'Polnisch', sv: 'Schwedisch', da: 'Dänisch', fi: 'Finnisch',
  nb: 'Norwegisch', tr: 'Türkisch', ar: 'Arabisch', hi: 'Hindi', cs: 'Tschechisch',
};

/**
 * Show voice suggestion banner for a given 2-letter language code.
 * Used when the user manually selects a translation target.
 */
function suggestVoiceForLang(langCode) {
  const prefix   = LANG_LOCALE_PREFIX[langCode] || (langCode + '-');
  const matching = allVoices.filter(v => v.locale.startsWith(prefix)).slice(0, 4);
  if (!matching.length) return;

  // Skip if the current voice already matches
  if (voiceInput.value && voiceInput.value.startsWith(prefix.replace(/-$/, ''))) return;

  const langName = LANG_NAMES[langCode] || langCode;
  const pills    = matching.map(v =>
    `<button class="lang-pill" data-voice="${escHtml(v.name)}" title="${escHtml(v.locale)} · ${escHtml(v.gender)}">${escHtml(v.name)}</button>`
  ).join('');

  langSuggest.innerHTML =
    `<span class="lang-flag">🌍</span>` +
    `<span class="lang-label">Übersetzung nach <strong>${escHtml(langName)}</strong> – empfohlene Stimmen:</span>` +
    `<span class="lang-voices">${pills}</span>` +
    `<button class="lang-dismiss" title="Schließen">✕</button>`;

  langSuggest.style.display = 'flex';

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
  const statusLabels = { queued: 'Wartend', processing: 'Läuft…', done: 'Fertig', error: 'Fehler', cancelled: 'Abgebrochen', resumable: 'Unterbrochen' };

  const selHint = job.status === 'queued' && (job.startPage || job.endPage || job.skipChapters?.length)
    ? `<span class="item-sel">✂ S.${job.startPage??'1'}–${job.endPage??'∞'}${job.skipChapters?.length ? ` · ${job.skipChapters.length} übersprungen` : ''}</span>`
    : '';

  el.innerHTML = `
    <div class="item-top">
      <span class="item-name" title="${escHtml(job.path)}">${escHtml(job.name)}</span>
      ${selHint}
      <span class="status status--${job.status}">${statusLabels[job.status] ?? job.status}</span>
      ${job.status === 'queued' ? `
        <button class="item-preview" data-preview="${job.id}" title="Seitenauswahl / Vorschau">🔍</button>
        <button class="item-remove"  data-remove="${job.id}"  title="Entfernen">✕</button>` : ''}
      ${job.status === 'resumable' ? `
        <button class="item-resume" data-resume="${job.id}" title="Konvertierung fortsetzen">▶ Fortsetzen</button>
        <button class="item-remove" data-remove="${job.id}"  title="Entfernen">✕</button>` : ''}
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
  el.querySelector('[data-preview]')?.addEventListener('click', e => {
    openPreview(e.currentTarget.dataset.preview);
  });
  el.querySelector('[data-remove]')?.addEventListener('click', e => {
    const id = e.currentTarget.dataset.remove;
    queue = queue.filter(j => j.id !== id);
    renderQueue();
  });
  el.querySelector('[data-resume]')?.addEventListener('click', e => {
    const id = e.currentTarget.dataset.resume;
    const j  = getJob(id);
    if (!j) return;
    j.status  = 'queued';
    j.resume  = true;
    j.subText = '';
    renderQueue();
    startConversion();
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
    job.subText  = settings.ttsEngine === 'piper' ? 'Piper TTS läuft…' : 'Verbinde mit Edge TTS…';
    renderQueue();
    updateJobEl(job);

    const opts = {
      jobId:        job.id,
      epubPath:     job.path,
      outputDir:    settings.outputDir || null,
      voice:        settings.voice,
      rate:         settings.rate,
      volume:       settings.volume,
      skipShort:    settings.skipShort,
      maxChapters:  previewMode ? 3 : null,
      merge:        settings.merge,
      createZip:    settings.createZip,
      startPage:    job.startPage   ?? null,
      endPage:      job.endPage     ?? null,
      skipChapters: job.skipChapters ?? [],
      translateTo:  settings.translateTo || null,
      resume:       job.resume || false,
      ttsEngine:    settings.ttsEngine,
      piperVoice:   settings.piperVoice,
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
      const isServerAbort = result.exitCode === 2;
      j.status  = isServerAbort ? 'resumable' : 'error';
      j.subText = result.error || 'Unbekannter Fehler';
      log(`✗ Fehler bei ${j.name}: ${j.subText}`, 'error');
      if (isServerAbort) log('▶ Konvertierung kann fortgesetzt werden wenn der Server wieder erreichbar ist.', 'warn');
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
