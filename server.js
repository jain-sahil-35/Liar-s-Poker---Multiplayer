const http = require('http'), fs = require('fs'), path = require('path');
const { WebSocketServer } = require('ws');
const PORT = process.env.PORT || 3000;
const page = path.join(__dirname, 'public', 'index.html');

const server = http.createServer((req, res) => {
  if (req.url === '/healthz') return res.end('ok');
  fs.readFile(page, (e, d) => {
    res.writeHead(e ? 500 : 200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(e ? 'error' : d);
  });
});
const wss = new WebSocketServer({ server });
const rooms = new Map();

// ---------- game logic ----------
const RANGE = { 1: [2, 14], 2: [2, 14], 3: [2, 14], 4: [2, 14], 5: [6, 14], 6: [6, 14], 7: [2, 14], 8: [2, 14], 9: [6, 13], 10: [0, 0] };
const key = b => [b.c, b.a || 0, b.b || 0];
const cmp = (x, y) => { for (let i = 0; i < 3; i++) if (x[i] !== y[i]) return x[i] - y[i]; return 0; };
function validBid(b) {
  if (!b || !Number.isInteger(b.c) || b.c < 1 || b.c > 10) return 'Invalid bid.';
  const [lo, hi] = RANGE[b.c], ok = r => Number.isInteger(r) && r >= lo && r <= hi;
  if (b.c < 10 && !ok(b.a)) return 'Invalid rank.';
  if ((b.c === 3 || b.c === 7) && !ok(b.b)) return 'Invalid rank.';
  if (b.c === 3 && b.a <= b.b) return 'Two Pair needs two different ranks, higher on top.';
  if (b.c === 7 && b.a === b.b) return 'A Full House needs two different ranks.';
  if ([6, 9, 10].includes(b.c) && !(Number.isInteger(b.s) && b.s >= 0 && b.s < 4)) return 'Invalid suit.';
  return null;
}
function truth(b, pool) {
  const n = {}; pool.forEach(([r]) => n[r] = (n[r] || 0) + 1);
  const c = r => n[r] || 0, has = (s, r) => pool.some(p => p[0] === r && p[1] === s);
  switch (b.c) {
    case 1: return c(b.a) >= 1;
    case 2: return c(b.a) >= 2;
    case 3: return c(b.a) >= 2 && c(b.b) >= 2;
    case 4: return c(b.a) >= 3;
    case 5: for (let i = 0; i < 5; i++) if (!c(b.a - i)) return false; return true;
    case 6: return has(b.s, b.a) && pool.filter(p => p[1] === b.s && p[0] <= b.a).length >= 5;
    case 7: return c(b.a) >= 3 && c(b.b) >= 2;
    case 8: return c(b.a) >= 4;
    case 9: for (let i = 0; i < 5; i++) if (!has(b.s, b.a - i)) return false; return true;
    default: for (let r = 10; r <= 14; r++) if (!has(b.s, r)) return false; return true;
  }
}
const alive = p => p.cards < 6;
function dealRound(R) {
  const d = [];
  for (let r = 2; r <= 14; r++) for (let s = 0; s < 4; s++) d.push([r, s]);
  for (let i = d.length - 1; i > 0; i--) { const j = Math.floor(Math.random() * (i + 1)); [d[i], d[j]] = [d[j], d[i]]; }
  let k = 0;
  R.players.forEach(p => { p.hand = alive(p) ? d.slice(k, k += p.cards) : []; });
  R.bid = null; R.bidBy = -1; R.result = null;
}
function nextAlive(R, i) {
  const n = R.players.length;
  for (let k = 1; k <= n; k++) { const j = (i + k) % n; if (alive(R.players[j])) return j; }
  return i;
}

// ---------- networking ----------
const send = (p, o) => { if (p.ws && p.ws.readyState === 1) p.ws.send(JSON.stringify(o)); };
function view(R, p) {
  const rev = R.phase === 'reveal' || R.phase === 'over';
  return {
    t: 'state', code: R.code, phase: R.phase, round: R.round, me: R.players.indexOf(p),
    isHost: R.host === p.id, turn: R.turn, bid: R.bid, bidBy: R.bidBy, result: R.result, winner: R.winner,
    hand: p.hand,
    players: R.players.map(x => ({ name: x.name, cards: x.cards, online: !!(x.ws && x.ws.readyState === 1), hand: rev ? x.hand : undefined })),
  };
}
const broadcast = R => { R.last = Date.now(); R.players.forEach(p => send(p, view(R, p))); };
const err = (ws, m) => ws.send(JSON.stringify({ t: 'err', m }));
const mkCode = () => {
  const a = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let c; do { c = Array.from({ length: 5 }, () => a[Math.floor(Math.random() * a.length)]).join(''); } while (rooms.has(c));
  return c;
};
const clean = n => String(n || '').trim().slice(0, 14) || 'Player';

wss.on('connection', ws => {
  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw); } catch { return; }
    if (m.t === 'create') {
      const R = { code: mkCode(), host: String(m.id), players: [], phase: 'lobby', round: 0, turn: 0, bid: null, bidBy: -1, result: null, winner: -1, last: Date.now() };
      rooms.set(R.code, R); m.t = 'join'; m.room = R.code;
    }
    if (m.t === 'join') {
      const R = rooms.get(String(m.room || '').toUpperCase());
      if (!R) return err(ws, 'Room not found.');
      let p = R.players.find(x => x.id === String(m.id));
      if (p) { if (p.ws && p.ws !== ws) try { p.ws.close(); } catch {} p.ws = ws; }
      else if (R.phase !== 'lobby') return err(ws, 'That game has already started.');
      else if (R.players.length >= 8) return err(ws, 'Room is full (8 players).');
      else { p = { id: String(m.id), name: clean(m.name), cards: 2, hand: [], ws }; R.players.push(p); }
      ws.p = p; ws.R = R; return broadcast(R);
    }
    const { p, R } = ws; if (!R) return;
    const i = R.players.indexOf(p);
    switch (m.t) {
      case 'start': {
        if (R.phase !== 'lobby' || R.host !== p.id) return;
        R.players = R.players.filter(x => x.ws && x.ws.readyState === 1);
        if (R.players.length < 2) return err(ws, 'Need at least 2 players online.');
        R.players.forEach(x => x.cards = 2);
        R.round = 1; R.phase = 'bidding'; R.turn = Math.floor(Math.random() * R.players.length); dealRound(R);
        return broadcast(R);
      }
      case 'bid': {
        if (R.phase !== 'bidding' || R.turn !== i) return;
        const e = validBid(m.bid); if (e) return err(ws, e);
        const b = { c: m.bid.c, a: m.bid.a || 0, b: m.bid.b || 0, s: m.bid.s || 0 };
        if (R.bid && cmp(key(b), key(R.bid)) <= 0) return err(ws, 'Your bid must be strictly stronger (suits add no strength).');
        R.bid = b; R.bidBy = i; R.turn = nextAlive(R, i); return broadcast(R);
      }
      case 'call': {
        if (R.phase !== 'bidding' || R.turn !== i || !R.bid) return;
        const pool = R.players.filter(alive).flatMap(x => x.hand);
        const valid = truth(R.bid, pool);
        R.result = { valid, caller: i, loser: valid ? i : R.bidBy }; R.phase = 'reveal'; return broadcast(R);
      }
      case 'next': {
        if (R.phase !== 'reveal' || !alive(p)) return;
        R.players[R.result.loser].cards++;
        const al = R.players.filter(alive);
        if (al.length <= 1) { R.phase = 'over'; R.winner = R.players.indexOf(al[0]); return broadcast(R); }
        let st = R.result.loser; while (!alive(R.players[st])) st = (st + 1) % R.players.length;
        R.turn = st; R.round++; R.phase = 'bidding'; dealRound(R); return broadcast(R);
      }
      case 'again': {
        if (R.phase !== 'over' || R.host !== p.id) return;
        R.players = R.players.filter(x => x.ws && x.ws.readyState === 1);
        R.players.forEach(x => { x.cards = 2; x.hand = []; }); R.phase = 'lobby'; return broadcast(R);
      }
      case 'leave': {
        if (R.phase === 'lobby') {
          R.players = R.players.filter(x => x !== p);
          if (R.host === p.id && R.players.length) R.host = R.players[0].id;
        }
        p.ws = null; ws.R = ws.p = null; return broadcast(R);
      }
    }
  });
  ws.on('close', () => { if (ws.p && ws.p.ws === ws) { ws.p.ws = null; broadcast(ws.R); } });
});

// drop rooms nobody has touched for 2 hours
setInterval(() => {
  for (const [c, R] of rooms) if (Date.now() - R.last > 2 * 3600e3 && !R.players.some(p => p.ws)) rooms.delete(c);
}, 10 * 60e3);

// keep connections alive through proxies/load balancers (they drop idle sockets)
setInterval(() => wss.clients.forEach(c => { if (c.readyState === 1) c.ping(); }), 25e3);

server.listen(PORT, () => console.log(`Liar's Poker running on http://localhost:${PORT}`));
