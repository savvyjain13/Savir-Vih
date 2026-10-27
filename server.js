// Savour Vih: Zombie Defense - multiplayer server for Render (Node 18+)
// Serves the game from /public and relays players' presence and events over WebSockets.
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const PUB = path.join(__dirname, 'public');
const TYPES = { '.html': 'text/html; charset=utf-8', '.js': 'application/javascript', '.css': 'text/css', '.png': 'image/png', '.ico': 'image/x-icon', '.svg': 'image/svg+xml' };
const MAX_PRESENCE = 8192;   // bytes of JSON per player
const MAX_MSG = 8192;        // bytes per relayed event
const MAX_PER_SEC = 120;     // messages per second per player

const server = http.createServer((req, res) => {
  let u = decodeURIComponent((req.url || '/').split('?')[0]);
  if (u === '/health') { res.writeHead(200, { 'Content-Type': 'text/plain' }); res.end('ok'); return; }
  if (u === '/') u = '/index.html';
  const file = path.normalize(path.join(PUB, u));
  if (!file.startsWith(PUB)) { res.writeHead(403); res.end(); return; }
  fs.readFile(file, (err, data) => {
    if (err) { res.writeHead(404, { 'Content-Type': 'text/plain' }); res.end('Not found'); return; }
    res.writeHead(200, { 'Content-Type': TYPES[path.extname(file)] || 'application/octet-stream', 'Cache-Control': 'no-cache' });
    res.end(data);
  });
});

const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 64 * 1024 });
const peers = new Map();
let seq = 0, dirty = false;

wss.on('connection', (ws) => {
  const id = 'p' + (++seq).toString(36) + Math.random().toString(36).slice(2, 6);
  const me = { id, ws, presence: {}, updatedAt: Date.now(), alive: true, count: 0, windowStart: Date.now() };
  peers.set(id, me);
  ws.send(JSON.stringify({ t: 'welcome', peer: id }));
  dirty = true;

  ws.on('pong', () => { me.alive = true; });
  ws.on('message', (raw) => {
    const now = Date.now();
    if (now - me.windowStart > 1000) { me.windowStart = now; me.count = 0; }
    if (++me.count > MAX_PER_SEC) return;
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    if (m.t === 'presence' && m.p && typeof m.p === 'object' && !Array.isArray(m.p)) {
      if (JSON.stringify(m.p).length > MAX_PRESENCE) return;
      me.presence = m.p; me.updatedAt = now; dirty = true;
    } else if (m.t === 'emit' && typeof m.topic === 'string' && m.topic.length < 64) {
      const out = JSON.stringify({ t: 'msg', topic: m.topic, data: m.data, peer: id });
      if (out.length > MAX_MSG) return;
      for (const p of peers.values()) if (p !== me && p.ws.readyState === 1) p.ws.send(out);
    }
  });
  ws.on('close', () => { peers.delete(id); dirty = true; });
  ws.on('error', () => {});
});

// Broadcast everyone's presence 20 times a second when something changed.
setInterval(() => {
  if (!dirty) return; dirty = false;
  const msg = JSON.stringify({ t: 'peers', peers: [...peers.values()].map(p => ({ peer: p.id, presence: p.presence, updatedAt: p.updatedAt })) });
  for (const p of peers.values()) if (p.ws.readyState === 1) p.ws.send(msg);
}, 50);

// Drop dead connections.
setInterval(() => {
  for (const p of peers.values()) {
    if (!p.alive) { p.ws.terminate(); continue; }
    p.alive = false; try { p.ws.ping(); } catch (e) {}
  }
}, 20000);

server.listen(PORT, () => console.log('Savour Vih server listening on port ' + PORT));
