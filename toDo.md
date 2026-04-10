# ToDo
- ~~übersetztes File speichern~~
- ~~favicon~~
- ~~speech.platform.bing.com ist immer mal wieder nicht erreichbar~~
  - ✅ Strategie 1 umgesetzt: 0,5 s Pause zwischen Chunks + Exponential Backoff (2 s, 4 s) bei Netzwerkfehlern, max. 3 Versuche pro Chunk
  - ✅ Nachproduktion: fehlgeschlagene Chunks werden nach 20 s Pause am Ende nochmals versucht
  - Falls Ausfälle zunehmen: längere max. Wartezeit im Backoff (z.B. 3. Versuch nach 30 s statt 4 s) und/oder `MAX_CONSECUTIVE` erhöhen
  - Strategie 2 (falls weiterhin Probleme): **Piper TTS** – lokal, kein Netz, gute deutsche Stimmen (`pip install piper-tts`, Voices: `de_DE-thorsten-high`, `de_DE-kerstin-medium`)
  - Strategie 3 (falls noch bessere Qualität gewünscht): **Kokoro / coqui-tts** – lokal, sehr hohe Qualität, deutsche Stimmen noch limitiert
- englische Variante