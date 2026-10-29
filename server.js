// Savour Vih: Zombie Defense - multiplayer server for Render (Node 18+)
// Serves the game from /public and relays players' presence and events.
// Two transports speak the same messages: a WebSocket, and HTTP long-polling at /lp for networks
// (schools, offices, hotels) whose filter refuses the upgrade handshake but allows ordinary HTTPS.
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const PUB = path.join(__dirname, 'public');
const TYPES = { '.html': 'text/html; charset=utf-8', '.js': 'application/javascript', '.css': 'text/css', '.png': 'image/png', '.ico': 'image/x-icon', '.svg': 'image/svg+xml', '.json': 'application/json; charset=utf-8' };
const MAX_PRESENCE = 8192;   // bytes of JSON per player
const MAX_MSG = 8192;        // bytes per relayed event
const MAX_PER_SEC = 120;     // messages per second per player
const LP_HOLD = 22000;       // how long a poll request waits when there is nothing to say
const LP_IDLE = 45000;       // a poller that has not asked in this long has closed its tab
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

const peers = new Map();
let seq = 0, dirty = false;

// A peer is either a socket or a poller; everything above this line only knows about `send` and `drop`.
function send(p, str) {
  if (p.ws) { if (p.ws.readyState === 1) p.ws.send(str); return; }
  p.box.push(str);
  if (p.wake) p.wake();
}
function drop(p) {
  peers.delete(p.id);
  if (p.ws) { try { p.ws.close(4001, 'kicked'); } catch (e) {} }
  if (p.wake) p.wake();
}
function newId() { return 'p' + (++seq).toString(36) + Math.random().toString(36).slice(2, 6); }
function blank(id, ws) {
  return { id, ws, box: ws ? null : [], wake: null, busy: false, presence: {}, updatedAt: Date.now(),
    lastReq: Date.now(), alive: true, count: 0, windowStart: Date.now(), admin: false, chatCount: 0, chatStart: Date.now() };
}

// The whole protocol, shared by both transports. `raw` is the JSON text one client message.
function handleMessage(me, raw) {
  const now = Date.now();
  if (now - me.windowStart > 1000) { me.windowStart = now; me.count = 0; }
  if (++me.count > MAX_PER_SEC) return;
  let m; try { m = JSON.parse(raw); } catch (e) { return; }
  if (m.t === 'presence' && m.p && typeof m.p === 'object' && !Array.isArray(m.p)) {
    if (JSON.stringify(m.p).length > MAX_PRESENCE) return;
    me.presence = m.p; me.updatedAt = now; dirty = true;
  } else if (m.t === 'emit' && typeof m.topic === 'string' && m.topic.length < 64) {
    const out = JSON.stringify({ t: 'msg', topic: m.topic, data: m.data, peer: me.id });
    if (out.length > MAX_MSG) return;
    for (const p of peers.values()) if (p !== me) send(p, out);
  } else if (m.t === 'auth') {
    const user = String(m.user || '').toLowerCase();
    me.admin = user === ADMIN_USER && String(m.pass || '') === ADMIN_PASS;
    send(me, JSON.stringify({ t: 'authok', admin: me.admin }));
    dirty = true;
  } else if (m.t === 'chat') {
    if (now - me.chatStart > CHAT_WINDOW) { me.chatStart = now; me.chatCount = 0; }
    if (++me.chatCount > CHAT_PER_WINDOW) return;
    const text = censor(cleanChat(m.text));
    if (!text) return;
    const name = String((me.presence && me.presence.n) || 'Survivor').slice(0, 16);
    const out = JSON.stringify({ t: 'chat', peer: me.id, name, text, admin: me.admin, at: now });
    for (const p of peers.values()) if (p !== me && hears(me, p)) send(p, out);
  } else if (m.t === 'purge') {
    if (!me.admin) return;
    for (const p of peers.values()) if (p !== me && hears(me, p)) send(p, JSON.stringify({ t: 'chat', purge: true, by: me.id }));
  } else if (m.t === 'kick') {
    const target = peers.get(String(m.peer || ''));
    if (!target || target === me) return;
    const mine = me.presence.hr, isHost = mine && mine.id && target.presence.rm === mine.id;
    if (!me.admin && !isHost) return;
    try { send(target, JSON.stringify({ t: 'kicked', by: me.id })); } catch (e) {}
    drop(target);
    for (const p of peers.values()) if (sameRoom(target, p)) send(p, JSON.stringify({ t: 'left', peer: target.id }));
  }
}

const server = http.createServer((req, res) => {
  let u = decodeURIComponent((req.url || '/').split('?')[0]);
  const reply = (code, type, body) => { try { res.writeHead(code, { 'Content-Type': type, 'Cache-Control': 'no-store' }); res.end(body); } catch (e) {} };
  if (u === '/health') { reply(200, 'text/plain', 'ok'); return; }
  if (u === '/lp') { if (req.method !== 'POST') { reply(405, 'text/plain', 'POST only'); return; } longPoll(req, res, reply); return; }
  if (u === '/') u = '/index.html';
  const file = path.normalize(path.join(PUB, u));
  if (!file.startsWith(PUB)) { try { res.writeHead(403); res.end(); } catch (e) {} return; }
  fs.readFile(file, (err, data) => {
    if (err) { reply(404, 'text/plain', 'Not found'); return; }
    try { res.writeHead(200, { 'Content-Type': TYPES[path.extname(file)] || 'application/octet-stream', 'Cache-Control': 'no-cache' }); res.end(data); } catch (e) {}
  });
});

function readBody(req) {
  return new Promise(done => {
    let n = 0; const parts = [];
    req.on('data', c => { n += c.length; if (n > 64 * 1024) { req.destroy(); done(null); return; } parts.push(c); });
    req.on('end', () => done(Buffer.concat(parts).toString('utf8')));
    req.on('error', () => done(null));
  });
}

// One request carries the poller's presence plus anything it wants to relay, and waits for a reply.
// An idle camp therefore costs one request per LP_HOLD per player, not one per second.
async function longPoll(req, res, reply) {
  const raw = await readBody(req);
  if (raw == null) { reply(400, 'application/json', '{"error":"body"}'); return; }
  let b; try { b = JSON.parse(raw); } catch (e) { reply(400, 'application/json', '{"error":"json"}'); return; }
  let p = b && b.peer ? peers.get(String(b.peer)) : null;
  if (p && p.ws) p = null;                       // a dead socket's id is not a ticket into someone else's camp
  const fresh = !p;
  if (fresh) { p = blank(newId(), null); peers.set(p.id, p); dirty = true; send(p, JSON.stringify({ t: 'welcome', peer: p.id })); }
  p.lastReq = Date.now();
  // Only a changed presence is worth a broadcast: re-sending the same one every poll would keep the whole
  // camp awake at 20Hz and turn a quiet lobby into a request storm.
  if (b.p && typeof b.p === 'object' && !Array.isArray(b.p) && JSON.stringify(b.p) !== JSON.stringify(p.presence)) {
    handleMessage(p, JSON.stringify({ t: 'presence', p: b.p }));
  }
  if (Array.isArray(b.s)) for (const m of b.s.slice(0, 40)) handleMessage(p, JSON.stringify(m));
  const hold = !p.busy;                          // a superseded poll drains itself so the new one owns the wait
  p.busy = true;
  const until = Date.now() + LP_HOLD;
  while (hold && !p.box.length && Date.now() < until) {
    await new Promise(r => { const to = setTimeout(() => { p.wake = null; r(); }, Math.max(50, until - Date.now())); p.wake = () => { clearTimeout(to); p.wake = null; r(); }; });
    if (!peers.has(p.id)) break;                 // kicked or swept while we were waiting
  }
  p.busy = false;
  const out = p.box.splice(0);
  reply(200, 'application/json', JSON.stringify({ peer: p.id, m: out.map(s => { try { return JSON.parse(s); } catch (e) { return null; } }).filter(Boolean) }));
}

const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 64 * 1024 });

wss.on('connection', (ws) => {
  const me = blank(newId(), ws);
  peers.set(me.id, me);
  send(me, JSON.stringify({ t: 'welcome', peer: me.id }));
  dirty = true;

  ws.on('pong', () => { me.alive = true; });
  ws.on('message', (raw) => handleMessage(me, raw));
  ws.on('close', () => { peers.delete(me.id); dirty = true; });
  ws.on('error', () => {});
});

// Broadcast everyone's presence 20 times a second when something changed.
setInterval(() => {
  if (!dirty) return; dirty = false;
  const msg = JSON.stringify({ t: 'peers', peers: [...peers.values()].map(p => ({ peer: p.id, presence: p.presence, updatedAt: p.updatedAt, admin: !!p.admin })) });
  for (const p of peers.values()) send(p, msg);
}, 50);

// Drop dead connections, and pollers whose tab went away without ever saying goodbye.
setInterval(() => {
  for (const p of peers.values()) {
    if (p.ws) { if (!p.alive) { p.ws.terminate(); continue; } p.alive = false; try { p.ws.ping(); } catch (e) {} }
    else if (Date.now() - p.lastReq > LP_IDLE) { if (p.wake) p.wake(); peers.delete(p.id); }
  }
}, 20000);

server.listen(PORT, () => console.log('Savour Vih server listening on port ' + PORT));
