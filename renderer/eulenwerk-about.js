/* Eulenwerk · Über-Dialog (Vanilla JS, keine Abhängigkeiten) · v1.0
 *
 *   <script src="eulenwerk-about.js"></script>
 *   <script>
 *     EulenwerkAbout.init({
 *       app: "homeScan", version: "1.3.0",
 *       description: "Heimnetz-Scanner mit interaktiver Netzwerkkarte.",
 *       stack: ["Node.js", "Express", "Leaflet"],
 *       logo: "icon.svg",                                     // eigenes App-Logo (optional; sonst Eule)
 *       github: "https://github.com/thunder312/homeScan"      // optional
 *     });
 *   </script>
 *   <button data-ew-about>Über</button>   ← jedes Element mit data-ew-about öffnet den Dialog
 *
 * Außerdem: EulenwerkAbout.open(), EulenwerkAbout.close(), EulenwerkAbout.owlSvg(className)
 */
(function (global) {
  "use strict";

  var OWL_INNER =
    '<path d="M8 5 C11 12 16 13 24 13 C28 13 30 15 32 16 C34 15 36 13 40 13 C48 13 53 12 56 5"/>' +
    '<circle cx="22" cy="28" r="9"/><circle cx="42" cy="28" r="9"/>' +
    '<circle fill="currentColor" stroke="none" cx="22" cy="28" r="3.2"/><circle fill="currentColor" stroke="none" cx="42" cy="28" r="3.2"/>' +
    '<path d="M29.5 39 L32 42.5 L34.5 39"/>' +
    '<path d="M13 28 V48 C13 54 18 59 24 59 H40 C46 59 51 54 51 48 V28"/>' +
    '<path d="M19 40 C18.5 48 19.5 54 23 59"/><path d="M45 40 C45.5 48 44.5 54 41 59"/>';

  function owlSvg(cls) {
    return '<svg class="' + (cls || "ew-logo") + '" viewBox="0 0 64 64" fill="none" stroke="currentColor" stroke-width="4" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">' + OWL_INNER + "</svg>";
  }

  var ICON_GITHUB = '<svg class="ew-icon" viewBox="0 0 16 16" fill="currentColor" stroke="none" aria-hidden="true"><path d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0016 8c0-4.42-3.58-8-8-8z"/></svg>';
  var ICON_BEER = '<svg class="ew-icon" viewBox="0 0 24 24" aria-hidden="true"><path d="M17 11h1a3 3 0 0 1 0 6h-1"/><path d="M9 12v6"/><path d="M13 12v6"/><path d="M14 7.5c-1 0-1.44.5-3 .5s-2-.5-3-.5-1.72.5-2.5.5a2.5 2.5 0 0 1 0-5c.78 0 1.57.5 2.5.5S9.44 2 11 2s2 1.5 3 1.5 1.72-.5 2.5-.5a2.5 2.5 0 0 1 0 5c-.78 0-1.5-.5-2.5-.5Z"/><path d="M5 8v12a2 2 0 0 0 2 2h8a2 2 0 0 0 2-2V8"/></svg>';
  var ICON_SPARK = '<svg class="ew-icon" viewBox="0 0 24 24" aria-hidden="true"><path d="M12 3v4M12 17v4M3 12h4M17 12h4M5.6 5.6l2.8 2.8M15.6 15.6l2.8 2.8M18.4 5.6l-2.8 2.8M8.4 15.6l-2.8 2.8"/></svg>';
  var ICON_X = '<svg class="ew-icon" viewBox="0 0 24 24" aria-hidden="true"><path d="M18 6 6 18M6 6l12 12"/></svg>';

  var cfg = null, dlg = null;

  function esc(s) { return String(s).replace(/[&<>"]/g, function (c) { return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]; }); }

  function build() {
    var year = new Date().getFullYear();
    var stack = (cfg.stack || []).map(function (s) { return '<span class="ew-badge">' + esc(s) + "</span>"; }).join("");
    var gh = cfg.github ? '<a href="' + esc(cfg.github) + '" target="_blank" rel="noopener">' + ICON_GITHUB + "Quellcode</a>" : "";
    var d = document.createElement("dialog");
    d.className = "ew-dialog ew-about";
    d.setAttribute("aria-labelledby", "ew-about-title");
    d.innerHTML =
      '<button type="button" class="ew-btn ew-btn--ghost ew-btn--icon ew-about__close" aria-label="Schließen" data-ew-about-close>' + ICON_X + "</button>" +
      '<div class="ew-about__head">' + (cfg.logo ? '<img class="ew-about__applogo" src="' + esc(cfg.logo) + '" alt="">' : owlSvg("ew-logo")) +
        "<div><h2 id=\"ew-about-title\">" + esc(cfg.app) + "</h2>" +
        '<p class="ew-muted">' + (cfg.version ? "Version " + esc(cfg.version) + " · " : "") + "Ein Eulenwerk-Projekt</p></div></div>" +
      '<div class="ew-about__body">' +
        (cfg.description ? '<p class="ew-about__desc">' + esc(cfg.description) + "</p>" : "") +
        "<h4>Entwickelt von</h4>" +
        '<div class="ew-about__people">' +
          '<div class="ew-about__person"><div class="ew-avatar">DE</div><div><strong>Daniel Ertl</strong><div class="ew-muted">Idee &amp; Entwicklung</div>' +
            '<div class="ew-about__links">' +
              '<a href="https://github.com/thunder312" target="_blank" rel="noopener">' + ICON_GITHUB + "github.com/thunder312</a>" +
              '<a href="https://buymeacoffee.com/dertl" target="_blank" rel="noopener">' + ICON_BEER + "Buy me a beer</a>" + gh +
            "</div></div></div>" +
          '<div class="ew-about__person"><div class="ew-avatar ew-avatar--ai">AI</div><div><strong>Claude</strong><div class="ew-muted">KI-Assistent beim Programmieren</div>' +
            '<div class="ew-about__links"><a href="https://claude.ai" target="_blank" rel="noopener">' + ICON_SPARK + "claude.ai</a></div></div></div>" +
        "</div>" +
        (stack ? "<h4>Technik</h4><div class=\"ew-about__stack\">" + stack + "</div>" : "") +
      "</div>" +
      '<div class="ew-about__foot">' +
        '<a class="ew-brand" href="https://daniel-ertl.de" target="_blank" rel="noopener">' + owlSvg("ew-logo") + "Eulenwerk – Software aus der Freizeit</a>" +
        "<span>© " + year + " Daniel Ertl</span>" +
      "</div>";
    d.addEventListener("click", function (e) {
      if (e.target === d || e.target.closest("[data-ew-about-close]")) d.close();
    });
    document.body.appendChild(d);
    return d;
  }

  function open() { if (!dlg) dlg = build(); if (!dlg.open) dlg.showModal(); }
  function close() { if (dlg && dlg.open) dlg.close(); }

  function init(options) {
    cfg = options || {};
    if (!cfg.app) cfg.app = document.title || "App";
    var bind = function () {
      document.querySelectorAll("[data-ew-about]").forEach(function (el) {
        if (el.__ewBound) return; el.__ewBound = true;
        el.addEventListener("click", function (e) { e.preventDefault(); open(); });
      });
    };
    if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", bind); else bind();
  }

  global.EulenwerkAbout = { init: init, open: open, close: close, owlSvg: owlSvg };
})(window);
