// Eulenwerk: Über-Dialog initialisieren (eigene Datei wegen CSP script-src 'self').
// Externe Links öffnen im Standardbrowser statt in einem neuen Electron-Fenster.
window.EulenwerkAbout.init({
  app: 'EPUB to Voice',
  version: '1.0.0',
  description: 'Wandelt E-Books (EPUB, PDF, TXT, DOCX) in Hörbücher um – mit Edge-TTS, Piper oder Qwen-TTS inklusive Stimmklonen, Kapitelerkennung und Fortsetzen abgebrochener Jobs.',
  stack: ['Electron', 'Python', 'edge-tts', 'Piper', 'Qwen-TTS'],
  github: 'https://github.com/thunder312/epubToVoice',
  logo: 'assets/icon.svg'
});
document.addEventListener('click', (e) => {
  const a = e.target.closest('a[href^="http"]');
  if (a && window.api?.revealPath) { e.preventDefault(); window.api.revealPath(a.href); }
});
