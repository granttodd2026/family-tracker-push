// Reminder sender. Runs on a schedule (GitHub Actions) for a few minutes at a time.
// It NEVER sees the family key. Phones send it, encrypted to SERVER_PUB over public Nostr relays:
//   d="sched"     {feed:{start,due,running}, every, meds:[{id,due,label}], tz}
//   d="sub:<dev>" {sub:<PushSubscription JSON>, prefs:{feed,meds}, name, tz, test?}
// Each family is identified only by the public key the phones derive from the family code (one-way hash).
// What has been sent is remembered in a state event encrypted to the server itself.
import WebSocket from 'ws';
import webpush from 'web-push';
import crypto from 'crypto';
import { finalizeEvent, getPublicKey } from 'nostr-tools/pure';
import * as nip44 from 'nostr-tools/nip44';

const RELAYS = (process.env.RELAYS || 'wss://relay.damus.io,wss://nos.lol,wss://relay.primal.net,wss://nostr.mom,wss://offchain.pub,wss://relay.nostr.net,wss://nostr.oxtr.dev').split(',');
const SK = Uint8Array.from(Buffer.from(process.env.SERVER_SK || '', 'hex'));
if (SK.length !== 32 || !process.env.VAPID_PUBLIC || !process.env.VAPID_PRIVATE) { console.error('missing SERVER_SK / VAPID keys'); process.exit(1); }
const PUB = getPublicKey(SK);
const RUN_MS = Number(process.env.RUN_SECONDS || 290) * 1000;
const TICK_MS = Number(process.env.TICK_SECONDS || 15) * 1000;
const MIN = 60000;
webpush.setVapidDetails(process.env.VAPID_SUBJECT || 'https://granttodd2026.github.io/family-tracker/', process.env.VAPID_PUBLIC, process.env.VAPID_PRIVATE);
const log = (...a) => console.log(new Date().toISOString(), ...a);
const h8 = (s) => crypto.createHash('sha256').update(s).digest('hex').slice(0, 16);

// ---- relay pool ----
const latest = new Map(); // `${pubkey}|${d}` -> event
let stateEv = null, eoseCount = 0;
const sockets = [];
function accept(ev) {
  if (ev.kind !== 30078) return;
  const d = (ev.tags.find((t) => t[0] === 'd') || [])[1]; if (!d) return;
  if (ev.pubkey === PUB) { if (d === 'state' && (!stateEv || ev.created_at > stateEv.created_at)) stateEv = ev; return; }
  if (!ev.tags.some((t) => t[0] === 'p' && t[1] === PUB)) return;
  const k = ev.pubkey + '|' + d, cur = latest.get(k);
  if (!cur || ev.created_at > cur.created_at || (ev.created_at === cur.created_at && ev.id < cur.id)) latest.set(k, ev);
}
function connect(url) {
  const ws = new WebSocket(url); const s = { url, ws, open: false };
  ws.on('open', () => { s.open = true; ws.send(JSON.stringify(['REQ', 'in', { kinds: [30078], '#p': [PUB] }])); ws.send(JSON.stringify(['REQ', 'st', { kinds: [30078], authors: [PUB], '#d': ['state'] }])); });
  ws.on('message', (m) => { let d; try { d = JSON.parse(m); } catch { return; } if (d[0] === 'EVENT') accept(d[2]); else if (d[0] === 'EOSE' && d[1] === 'in') eoseCount++; });
  ws.on('error', () => {}); ws.on('close', () => { s.open = false; });
  sockets.push(s);
}
RELAYS.forEach(connect);
const publish = (ev) => { for (const s of sockets) if (s.open) try { s.ws.send(JSON.stringify(['EVENT', ev])); } catch {} };

// ---- state ----
const selfKey = nip44.getConversationKey(SK, PUB);
let state = { sent: {}, dead: {} }, dirty = false, lastStateCa = 0;
function loadState() { if (stateEv) try { state = { sent: {}, dead: {}, ...JSON.parse(nip44.decrypt(stateEv.content, selfKey)) }; lastStateCa = stateEv.created_at; } catch (e) { log('state decrypt failed', e.message); } }
function saveState() {
  const now = Date.now();
  for (const sp of Object.keys(state.sent)) { for (const [k, t] of Object.entries(state.sent[sp])) if (now - t > 3 * 86400000) delete state.sent[sp][k]; if (!Object.keys(state.sent[sp]).length) delete state.sent[sp]; }
  for (const [k, t] of Object.entries(state.dead)) if (now - t > 30 * 86400000) delete state.dead[k];
  const ca = Math.max(Math.floor(now / 1000), lastStateCa + 1); lastStateCa = ca;
  publish(finalizeEvent({ kind: 30078, created_at: ca, tags: [['d', 'state']], content: nip44.encrypt(JSON.stringify(state), selfKey) }, SK));
  dirty = false;
}

// ---- alerts ----
const fmt = (ts, tz) => { try { return new Date(ts).toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit', timeZone: tz || 'UTC' }); } catch { return new Date(ts).toISOString().slice(11, 16) + ' UTC'; } };
function alertsFor(sched, now) {
  const out = [];
  if (!sched) return out;
  const tz = sched.tz;
  const f = sched.feed;
  if (f && f.due && !f.running) {
    if (now >= f.due - 15 * MIN && now < f.due - 30000) out.push({ key: `f:${f.start}:15`, kind: 'feed', ttl: 900, msg: { title: `Nursing due in ${Math.max(1, Math.round((f.due - now) / MIN))} min`, body: `Due at ${fmt(f.due, tz)}`, tag: 'feed', data: { feedStart: f.start } } });
    else if (now >= f.due - 30000 && now < f.due + 45 * MIN) out.push({ key: `f:${f.start}:0`, kind: 'feed', ttl: 3600, msg: { title: 'Nursing due now', body: `Last feed started ${fmt(f.start, tz)}`, tag: 'feed', data: { feedStart: f.start } } });
  }
  for (const m of sched.meds || []) if (m && m.due && now >= m.due - 30000 && now < m.due + 60 * MIN) out.push({ key: `m:${m.id}:${m.due}`, kind: 'meds', ttl: 3600, msg: { title: String(m.label || 'Med reminder').slice(0, 80), body: 'Tap to log it in the tracker', tag: 'med-' + m.id, data: {} } });
  return out;
}
async function send(sub, msg, ttl) {
  try { await webpush.sendNotification(sub, JSON.stringify(msg), { TTL: ttl, urgency: 'high' }); return 'ok'; }
  catch (e) { if (e.statusCode === 404 || e.statusCode === 410) return 'dead'; log('push error', e.statusCode || '', (e.body || e.message || '').toString().slice(0, 120)); return 'err'; }
}
// If another runner (e.g. box + Actions) saved newer state, merge it so nothing is sent twice.
function mergeNewerState() {
  if (!stateEv || stateEv.created_at <= lastStateCa) return;
  try {
    const o = JSON.parse(nip44.decrypt(stateEv.content, selfKey));
    for (const [sp, m] of Object.entries(o.sent || {})) { state.sent[sp] ||= {}; for (const [k, t] of Object.entries(m)) if (!state.sent[sp][k]) state.sent[sp][k] = t; }
    for (const [k, t] of Object.entries(o.dead || {})) state.dead[k] ||= t;
    lastStateCa = stateEv.created_at;
  } catch (e) { log('state merge failed', e.message); }
}
async function tick() {
  mergeNewerState();
  const now = Date.now();
  const spaces = new Map();
  for (const [k, ev] of latest) { const [pk, d] = k.split('|'); if (!spaces.has(pk)) spaces.set(pk, { sched: null, subs: [] }); const sp = spaces.get(pk); let obj; try { obj = JSON.parse(nip44.decrypt(ev.content, nip44.getConversationKey(SK, pk))); } catch { continue; } if (d === 'sched') sp.sched = obj; else if (d.startsWith('sub:') && !obj.off && obj.sub && obj.sub.endpoint && !state.dead[h8(obj.sub.endpoint)]) sp.subs.push({ dev: d.slice(4), ...obj }); }
  for (const [pk, sp] of spaces) {
    if (!sp.subs.length) continue;
    const sent = (state.sent[pk] ||= {});
    const sid = pk.slice(0, 8);
    for (const s of sp.subs) if (s.test && now - s.test < 15 * MIN && !sent['t:' + s.dev + ':' + s.test]) {
      const r = await send(s.sub, { title: 'Test notification ✓', body: 'Reminders are working on this phone.', tag: 'test', data: {} }, 900);
      sent['t:' + s.dev + ':' + s.test] = now; dirty = true; log(sid, 'test ->', s.dev.slice(0, 6), r);
      if (r === 'dead') state.dead[h8(s.sub.endpoint)] = now;
    }
    for (const a of alertsFor(sp.sched, now)) {
      if (sent[a.key]) continue;
      const targets = sp.subs.filter((s) => !s.prefs || s.prefs[a.kind] !== false);
      if (!targets.length) continue;
      sent[a.key] = now; dirty = true;
      for (const s of targets) { const r = await send(s.sub, a.msg, a.ttl); log(sid, a.key, '->', s.dev.slice(0, 6), r); if (r === 'dead') state.dead[h8(s.sub.endpoint)] = now; }
    }
  }
  if (dirty) saveState();
}

// ---- main loop ----
const t0 = Date.now();
while (eoseCount < Math.min(3, RELAYS.length) && Date.now() - t0 < 10000) await new Promise((r) => setTimeout(r, 200));
loadState();
log(`server ${PUB.slice(0, 8)} ready: ${sockets.filter((s) => s.open).length}/${RELAYS.length} relays, ${latest.size} inbound events`);
while (Date.now() - t0 < RUN_MS) {
  try { await tick(); } catch (e) { log('tick error', e); }
  await new Promise((r) => setTimeout(r, TICK_MS));
}
if (dirty) saveState();
await new Promise((r) => setTimeout(r, 1500));
for (const s of sockets) try { s.ws.close(); } catch {}
log('done'); process.exit(0);
