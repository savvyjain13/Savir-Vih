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
const ADMIN_USER = (process.env.ADMIN_USER || 'Sapnap').toLowerCase();
const ADMIN_PASS = process.env.ADMIN_PASS || '2132';
const CHAT_MAX = 160;
const CHAT_WINDOW = 3000;
const CHAT_PER_WINDOW = 5;
const CHAT_BAD = ['fuck', 'shit', 'bitch', 'cunt', 'asshole', 'bastard', 'dick', 'pussy', 'slut', 'whore', 'nigger', 'nigga', 'faggot', 'retard'];
const cleanChat = s => String(s || '').replace(/[\u0000-\u001f\u007f]/g, ' ').replace(/\s+/g, ' ').trim().slice(0, CHAT_MAX);
const censor = s => {
  let out = s;
  for (const w of CHAT_BAD) out = out.replace(new RegExp(w.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'gi'), m => m[0] + '*'.repeat(Math.max(0, m.length - 2)) + (m.length > 1 ? m[m.length - 1] : ''));
  return out;
};
const sameRoom = (a, b) => !!(a.presence.rm && a.presence.rm === b.presence.rm);
// Party members hear their own party; players standing in the open main camp hear the whole camp.
const hears = (a, b) => (a.presence.rm ? a.presence.rm === b.presence.rm : !b.presence.rm);

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
  const me = { id, ws, presence: {}, updatedAt: Date.now(), alive: true, count: 0, windowStart: Date.now(), admin: false, chatCount: 0, chatStart: Date.now() };
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
    } else if (m.t === 'auth') {
      const user = String(m.user || '').toLowerCase();
      me.admin = user === ADMIN_USER && String(m.pass || '') === ADMIN_PASS;
      ws.send(JSON.stringify({ t: 'authok', admin: me.admin }));
      dirty = true;
    } else if (m.t === 'chat') {
      if (now - me.chatStart > CHAT_WINDOW) { me.chatStart = now; me.chatCount = 0; }
      if (++me.chatCount > CHAT_PER_WINDOW) return;
      const text = censor(cleanChat(m.text));
      if (!text) return;
      const name = String((me.presence && me.presence.n) || 'Survivor').slice(0, 16);
      const out = JSON.stringify({ t: 'chat', peer: id, name, text, admin: me.admin, at: now });
      for (const p of peers.values()) if (p !== me && p.ws.readyState === 1 && hears(me, p)) p.ws.send(out);
    } else if (m.t === 'purge') {
      if (!me.admin) return;
      for (const p of peers.values()) if (p !== me && p.ws.readyState === 1 && hears(me, p)) p.ws.send(JSON.stringify({ t: 'chat', purge: true, by: id }));
    } else if (m.t === 'kick') {
      const target = peers.get(String(m.peer || ''));
      if (!target || target === me) return;
      const mine = me.presence.hr, isHost = mine && mine.id && target.presence.rm === mine.id;
      if (!me.admin && !isHost) return;
      try { target.ws.send(JSON.stringify({ t: 'kicked', by: id })); } catch (e) {}
      target.ws.close(4001, 'kicked');
      for (const p of peers.values()) if (p.ws.readyState === 1 && sameRoom(target, p)) p.ws.send(JSON.stringify({ t: 'left', peer: target.id }));
    }
  });
  ws.on('close', () => { peers.delete(id); dirty = true; });
  ws.on('error', () => {});
});

// Broadcast everyone's presence 20 times a second when something changed.
setInterval(() => {
  if (!dirty) return; dirty = false;
  const msg = JSON.stringify({ t: 'peers', peers: [...peers.values()].map(p => ({ peer: p.id, presence: p.presence, updatedAt: p.updatedAt, admin: !!p.admin })) });
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
