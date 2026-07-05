// The M6 demo console (M6_PLAN.md "Console"). A single self-contained
// HTML+JS page served at `GET /console`, plus the shared browser-side frame
// decoder it uses. The console consumes ONLY the existing gateway HTTP API
// (db list/create/fork/manifest/dials/gc/objects + the stream proxy) — the
// §10.2 "the GUI is just another stream listener" demo, no Postgres anywhere.
//
// The decoder below is a ~80-line mirror of pglite-cell's frame codec
// (packages/pglite-cell/src/frames.ts). It decodes ONLY headers — for W
// frames it reads the u16 hdrLen + header JSON and skips the trailing wal
// bytes (recording only their length). It is exported as a pure function so
// the test suite can prove parity against the real `encodeAppend` without a
// browser, and the CONSOLE_HTML string embeds the identical source inline.

// ---------------------------------------------------------------------------
// Shared browser-side decoder (pure, no DOM) — tested for codec parity.
// ---------------------------------------------------------------------------

/** A frame as seen by the browser feed: type, header JSON, W wal byte count. */
export interface DecodedFrame {
  type: string
  header: Record<string, unknown>
  /** Byte length of the trailing wal payload (W frames only), else 0. */
  walBytes: number
}

/** One CAS append group: its start offset plus the frames sharing it. */
export interface DecodedGroup {
  offset: string
  frames: DecodedFrame[]
}

/**
 * Decode a concatenated byte-mode stream body into append groups. Mirrors the
 * `type | u32len | payload` layout and W's `u16 hdrLen | json | wal` header;
 * groups consecutive frames by their shared `expectedOffset` (one CAS append).
 * Returns whole groups plus the count of bytes consumed (a trailing partial
 * frame is left unconsumed for the caller to re-feed with more bytes).
 */
export function decodeStreamBytes(bytes: Uint8Array): {
  groups: DecodedGroup[]
  consumed: number
} {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)
  const dec = new TextDecoder()
  const groups: DecodedGroup[] = []
  let pos = 0

  // Decode one frame at `p`, or null if the buffer holds only a partial frame.
  function frameAt(p: number): { frame: DecodedFrame; next: number } | null {
    if (p + 5 > bytes.length) return null
    const type = String.fromCharCode(bytes[p])
    const payloadLen = view.getUint32(p + 1, false)
    const payloadStart = p + 5
    const payloadEnd = payloadStart + payloadLen
    if (payloadEnd > bytes.length) return null
    let header: Record<string, unknown>
    let walBytes = 0
    if (type === 'W') {
      const hdrLen = view.getUint16(payloadStart, false)
      const headerStart = payloadStart + 2
      const headerEnd = headerStart + hdrLen
      header = JSON.parse(dec.decode(bytes.subarray(headerStart, headerEnd)))
      walBytes = payloadEnd - headerEnd
    } else {
      header = JSON.parse(dec.decode(bytes.subarray(payloadStart, payloadEnd)))
    }
    return { frame: { type, header, walBytes }, next: payloadEnd }
  }

  for (;;) {
    const first = frameAt(pos)
    if (!first) break
    const expected = first.frame.header.expectedOffset
    const frames: DecodedFrame[] = [first.frame]
    let end = first.next
    let complete = false
    for (;;) {
      const nxt = frameAt(end)
      if (!nxt) {
        // No further whole frame. If we sit exactly at the buffer end the run
        // is complete; otherwise a partial same-offset frame may follow — wait.
        complete = end === bytes.length
        break
      }
      if (nxt.frame.header.expectedOffset !== expected) {
        complete = true
        break
      }
      frames.push(nxt.frame)
      end = nxt.next
    }
    if (!complete) break
    groups.push({ offset: String(expected ?? ''), frames })
    pos = end
  }
  return { groups, consumed: pos }
}

// ---------------------------------------------------------------------------
// The served page.
// ---------------------------------------------------------------------------

// NOTE: the decoder above is duplicated inline in the page script below so the
// page is fully self-contained (no module loading in the browser). The tests
// assert parity of `decodeStreamBytes` against the real encoder; the inline
// copy is kept byte-identical in logic.

/** The single-file console page (served verbatim at `GET /console`). */
export const CONSOLE_HTML = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>PGlite Gateway Console</title>
<style>
  :root {
    --bg: #0d1117; --panel: #161b22; --border: #30363d; --fg: #c9d1d9;
    --muted: #8b949e; --accent: #58a6ff; --green: #3fb950; --yellow: #d29922;
    --red: #f85149; --purple: #bc8cff; --mono: ui-monospace, SFMono-Regular,
    "SF Mono", Menlo, Consolas, monospace;
  }
  * { box-sizing: border-box; }
  body {
    margin: 0; background: var(--bg); color: var(--fg);
    font: 14px/1.5 system-ui, -apple-system, sans-serif;
  }
  header {
    padding: 12px 18px; border-bottom: 1px solid var(--border);
    display: flex; align-items: baseline; gap: 14px;
  }
  header h1 { font-size: 16px; margin: 0; }
  header .info { color: var(--muted); font: 12px var(--mono); }
  .layout { display: grid; grid-template-columns: 280px 1fr; gap: 0;
    height: calc(100vh - 50px); }
  .left { border-right: 1px solid var(--border); overflow-y: auto;
    padding: 12px; }
  .main { display: flex; flex-direction: column; overflow: hidden; }
  .detail { padding: 16px; overflow-y: auto; flex: 0 0 auto;
    max-height: 55%; }
  .feed-wrap { border-top: 1px solid var(--border); flex: 1 1 auto;
    display: flex; flex-direction: column; overflow: hidden; }
  .feed-head { padding: 8px 16px; border-bottom: 1px solid var(--border);
    display: flex; align-items: center; gap: 10px; }
  .feed { overflow-y: auto; flex: 1; font: 12px var(--mono); padding: 4px 0; }
  h2 { font-size: 13px; text-transform: uppercase; letter-spacing: .06em;
    color: var(--muted); margin: 0 0 8px; }
  button {
    background: var(--panel); color: var(--fg); border: 1px solid var(--border);
    border-radius: 6px; padding: 5px 10px; cursor: pointer; font-size: 12px;
  }
  button:hover { border-color: var(--accent); }
  button.primary { background: var(--accent); color: #06131f;
    border-color: var(--accent); font-weight: 600; }
  input, textarea {
    background: var(--bg); color: var(--fg); border: 1px solid var(--border);
    border-radius: 6px; padding: 5px 8px; font-size: 12px; width: 100%;
    font-family: var(--mono);
  }
  .db-item { padding: 7px 9px; border: 1px solid transparent; border-radius: 6px;
    cursor: pointer; display: flex; justify-content: space-between;
    align-items: center; gap: 6px; }
  .db-item:hover { background: var(--panel); }
  .db-item.sel { background: var(--panel); border-color: var(--accent); }
  .db-item .name { font-weight: 600; }
  .db-item .meta { color: var(--muted); font: 11px var(--mono); }
  .db-item button { padding: 2px 7px; font-size: 11px; }
  form.create { margin: 12px 0; display: flex; gap: 6px; }
  form.create input { flex: 1; }
  table.facts { width: 100%; border-collapse: collapse; }
  table.facts td { padding: 3px 8px 3px 0; vertical-align: top; }
  table.facts td.k { color: var(--muted); white-space: nowrap; width: 1px; }
  table.facts td.v { font-family: var(--mono); word-break: break-all; }
  .row { display: flex; gap: 8px; flex-wrap: wrap; align-items: center;
    margin: 10px 0; }
  .card { background: var(--panel); border: 1px solid var(--border);
    border-radius: 8px; padding: 12px 14px; margin-bottom: 14px; }
  .frow { display: grid;
    grid-template-columns: 92px 150px 60px 1fr; gap: 10px; padding: 3px 16px;
    border-bottom: 1px solid #21262d; }
  .frow:hover { background: var(--panel); }
  .frow .time { color: var(--muted); }
  .frow .off { color: var(--muted); overflow: hidden; text-overflow: ellipsis;
    white-space: nowrap; }
  .letters { font-weight: 700; }
  .L-W { color: var(--accent); } .L-K { color: var(--green); }
  .L-L { color: var(--yellow); } .L-G { color: var(--purple); }
  .L-N { color: var(--purple); } .L-S, .L-O { color: var(--red); }
  .summ { color: var(--fg); }
  .summ .part { margin-right: 12px; }
  .frow.era { background: rgba(248,81,73,.10); }
  .pill { display: inline-block; padding: 1px 7px; border-radius: 10px;
    font: 11px var(--mono); border: 1px solid var(--border); }
  .pill.on { color: var(--green); border-color: var(--green); }
  .pill.off { color: var(--muted); }
  #toasts { position: fixed; right: 16px; bottom: 16px; display: flex;
    flex-direction: column; gap: 8px; z-index: 50; }
  .toast { background: var(--panel); border: 1px solid var(--purple);
    border-left-width: 4px; border-radius: 6px; padding: 8px 12px;
    max-width: 320px; font-size: 12px; box-shadow: 0 4px 14px #0008;
    animation: fade .3s ease; }
  .toast b { color: var(--purple); }
  @keyframes fade { from { opacity: 0; transform: translateY(6px); } }
  .empty { color: var(--muted); padding: 20px; text-align: center; }
  a { color: var(--accent); }
</style>
</head>
<body>
<header>
  <h1>PGlite Gateway Console</h1>
  <span class="info" id="conn"></span>
</header>
<div class="layout">
  <aside class="left">
    <h2>Databases</h2>
    <form class="create" id="createForm">
      <input id="createName" placeholder="new database name" autocomplete="off" />
      <button class="primary" type="submit">Create</button>
    </form>
    <div id="dbList"></div>
  </aside>
  <main class="main">
    <div class="detail" id="detail">
      <div class="empty">Select a database.</div>
    </div>
    <div class="feed-wrap">
      <div class="feed-head">
        <h2 style="margin:0">Live frame feed</h2>
        <span class="pill off" id="feedStatus">idle</span>
        <span class="info" id="feedEra"></span>
      </div>
      <div class="feed" id="feed">
        <div class="empty">Select a database to tail its era stream.</div>
      </div>
    </div>
  </main>
</div>
<div id="toasts"></div>
<script>
${decoderSource()}

// --- state --------------------------------------------------------------
var selected = null;      // selected database id
var feedGen = 0;          // increments to cancel a running feed loop
var INITIAL = "0000000000000000_0000000000000000";

function h(s){ return String(s == null ? "" : s)
  .replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;"); }
function shortId(s){ s = String(s || ""); return s.length > 10 ? s.slice(0,8) : s; }
function shortOff(s){ s = String(s || ""); var p = s.split("_");
  return p.length === 2 ? p[0].replace(/^0+/,"0") + "_" + p[1].replace(/^0+/,"0") : s; }

// --- api ----------------------------------------------------------------
async function api(path, opts){ var r = await fetch(path, opts);
  if(!r.ok) throw new Error(path + " -> " + r.status); return r; }
async function jget(path){ return (await api(path)).json(); }

async function loadConn(){
  try { var info = await jget("/v1/console-info");
    document.getElementById("conn").textContent =
      typeof info === "string" ? info : JSON.stringify(info);
  } catch(e){}
}

async function refreshList(){
  var dbs;
  try { dbs = await jget("/v1/db"); } catch(e){ return; }
  var el = document.getElementById("dbList");
  if(!dbs.length){ el.innerHTML = '<div class="empty">No databases yet.</div>'; return; }
  var manifests = {};
  await Promise.all(dbs.map(async function(d){
    try { manifests[d.id] = await jget("/v1/db/" + d.id + "/manifest"); } catch(e){}
  }));
  el.innerHTML = dbs.map(function(d){
    var m = manifests[d.id]; var sel = d.id === selected ? " sel" : "";
    var meta = m ? ("era " + m.era.ordinal + " · ckpt " + h(m.checkpoint.lsn)) : h(d.status);
    return '<div class="db-item' + sel + '" data-id="' + h(d.id) + '">' +
      '<div><div class="name">' + h(d.name) + '</div>' +
      '<div class="meta">' + meta + '</div></div>' +
      '<button data-fork="' + h(d.id) + '">fork</button></div>';
  }).join("");
}

// --- detail -------------------------------------------------------------
async function showDetail(id){
  var d = document.getElementById("detail");
  var m, ckpt = null, pins = [];
  try { m = await jget("/v1/db/" + id + "/manifest"); }
  catch(e){ d.innerHTML = '<div class="empty">manifest error</div>'; return; }
  try { ckpt = await jget("/v1/db/" + id + "/checkpoint/latest"); } catch(e){}
  try { pins = await jget("/v1/db/" + id + "/pins"); } catch(e){}
  var stats = null;
  try { stats = await jget("/v1/db/" + id + "/stats"); } catch(e){}
  var size = null;
  if(ckpt && ckpt.objectRef){
    try { var hr = await fetch("/v1/objects/" + ckpt.objectRef, { method: "HEAD" });
      var cl = hr.headers.get("content-length"); if(cl) size = Number(cl); } catch(e){}
  }
  var dials = m.dials;
  d.innerHTML =
    '<div class="card"><h2>' + h(m.name) + '</h2><table class="facts">' +
    fact("database id", m.databaseId) +
    fact("era", m.era.ordinal + "  " + h(m.era.path)) +
    fact("era base", h(m.era.baseLsn) + " @ " + h(shortOff(m.era.baseOffset))) +
    fact("checkpoint lsn", ckpt ? h(ckpt.lsn) : h(m.checkpoint.lsn)) +
    fact("checkpoint ref", h((ckpt ? ckpt.objectRef : m.checkpoint.ref) || "")) +
    fact("checkpoint offset", h(shortOff((ckpt ? ckpt.streamOffset : m.checkpoint.streamOffset)))) +
    (size != null ? fact("checkpoint size", fmtBytes(size)) : "") +
    ((stats && stats.latestCheckpoint && stats.latestCheckpoint.eagerBytes != null) ?
      fact("eager set", fmtBytes(stats.latestCheckpoint.eagerBytes) +
        " (what a cold wake moves)") +
      fact("lazy set", fmtBytes(stats.latestCheckpoint.lazyBytes || 0)) +
      fact("file count", String(stats.latestCheckpoint.fileCount)) : "") +
    "</table></div>" +
    '<div class="card"><h2>Dials</h2>' +
    '<table class="facts">' +
    '<tr><td class="k">checkpointEveryBytes</td><td class="v">' +
      '<input id="d_ckpt" value="' + h(dials.checkpointEveryBytes) + '" /></td></tr>' +
    '<tr><td class="k">rotateEveryBytes</td><td class="v">' +
      '<input id="d_rot" value="' + h(dials.rotateEveryBytes) + '" /></td></tr>' +
    '<tr><td class="k">gcGraceMs</td><td class="v">' +
      '<input id="d_gc" value="' + h(dials.gcGraceMs) + '" /></td></tr>' +
    '</table><div class="row">' +
    '<button class="primary" id="saveDials">Save dials</button>' +
    '<button id="gcBtn">GC now</button>' +
    '<button id="refreshBtn">Refresh</button></div></div>' +
    '<div class="card"><h2>Pins (' + pins.length + ')</h2>' +
    (pins.length ? '<table class="facts">' + pins.map(function(p){
      return fact(h(p.kind) + " / " + h(p.holder),
        h(p.pinnedLsn) + " @ " + h(shortOff(p.pinnedOffset))); }).join("") + "</table>"
      : '<div class="meta" style="color:var(--muted)">none</div>') + "</div>";

  document.getElementById("saveDials").onclick = async function(){
    await api("/v1/db/" + id + "/dials", { method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        checkpointEveryBytes: document.getElementById("d_ckpt").value,
        rotateEveryBytes: document.getElementById("d_rot").value,
        gcGraceMs: document.getElementById("d_gc").value }) });
    showDetail(id);
  };
  document.getElementById("gcBtn").onclick = async function(){
    var r = await (await api("/v1/db/" + id + "/gc", { method: "POST" })).json();
    toast("GC", JSON.stringify(r));
  };
  document.getElementById("refreshBtn").onclick = function(){ showDetail(id); };
}
function fact(k, v){ return '<tr><td class="k">' + h(k) + '</td><td class="v">' + v + "</td></tr>"; }
function fmtBytes(n){ if(n < 1024) return n + " B";
  if(n < 1048576) return (n/1024).toFixed(1) + " KB"; return (n/1048576).toFixed(2) + " MB"; }

// --- select -------------------------------------------------------------
async function select(id){
  selected = id; feedGen++;                 // cancel any running feed
  document.querySelectorAll(".db-item").forEach(function(e){
    e.classList.toggle("sel", e.getAttribute("data-id") === id); });
  await showDetail(id);
  startFeed(id);
}

// --- live feed ----------------------------------------------------------
async function startFeed(id){
  var gen = ++feedGen;
  var feed = document.getElementById("feed");
  feed.innerHTML = "";
  var status = document.getElementById("feedStatus");
  var eraLabel = document.getElementById("feedEra");
  // Start tailing from the era's base offset (the era stream path from the
  // manifest). This is the flagship "GUI is just another stream listener".
  var m;
  try { m = await jget("/v1/db/" + id + "/manifest"); }
  catch(e){ return; }
  var streamPath = m.era.path;              // e.g. /era/000001-XXXX
  var offset = INITIAL;                      // the era's initial read token
  status.textContent = "live"; status.className = "pill on";

  while(gen === feedGen){
    eraLabel.textContent = streamPath;
    var url = "/v1/db/" + id + "/stream" + streamPath +
      "?offset=" + encodeURIComponent(offset) + "&live=long-poll";
    var res;
    try { res = await fetch(url); }
    catch(e){ await sleep(1000); continue; }
    if(gen !== feedGen) return;
    if(res.status === 204){ continue; }       // long-poll timeout -> re-poll
    if(!res.ok){ await sleep(1000); continue; }
    var next = res.headers.get("Stream-Next-Offset");
    var bytes = new Uint8Array(await res.arrayBuffer());
    if(bytes.length){
      var out = decodeStreamBytes(bytes);
      var hopped = false;
      for(var i = 0; i < out.groups.length; i++){
        renderGroup(feed, out.groups[i]);
        // Era hop: on a terminal S frame, switch to the next era stream path
        // (through the same proxy prefix) and reset the offset token.
        var s = out.groups[i].frames.find(function(f){ return f.type === "S"; });
        if(s && s.header.nextEraUrl){
          streamPath = s.header.nextEraUrl;
          offset = INITIAL;
          hopped = true;
        }
      }
      if(hopped){ continue; }                 // re-poll on the new era path
    }
    if(next) offset = next;
  }
}
function sleep(ms){ return new Promise(function(r){ setTimeout(r, ms); }); }

function renderGroup(feed, group){
  var letters = group.frames.map(function(f){ return f.type; }).join("");
  var isEra = group.frames.some(function(f){ return f.type === "S" || f.type === "O"; });
  var parts = group.frames.map(frameSummary).join("");
  var lettersHtml = group.frames.map(function(f){
    return '<span class="L-' + f.type + '">' + f.type + "</span>"; }).join("");
  var row = document.createElement("div");
  row.className = "frow" + (isEra ? " era" : "");
  row.innerHTML =
    '<span class="time">' + new Date().toLocaleTimeString() + "</span>" +
    '<span class="off">' + h(shortOff(group.offset)) + "</span>" +
    '<span class="letters">' + lettersHtml + "</span>" +
    '<span class="summ">' + parts + "</span>";
  feed.appendChild(row);
  feed.scrollTop = feed.scrollHeight;
  // N frames also surface as toasts.
  group.frames.forEach(function(f){
    if(f.type === "N") toast(h(f.header.channel), h(f.header.payload)); });
}

function frameSummary(f){
  var hd = f.header, s;
  if(f.type === "W"){
    s = h(hd.kind) + " " + shortId(hd.commitId) + " " +
        h(hd.baseLsn) + "→" + h(hd.endLsn) + " (" + f.walBytes + "B)";
  } else if(f.type === "K"){
    s = "checkpoint lsn " + h(hd.lsn);
  } else if(f.type === "L"){
    s = "lease " + h(hd.kind) + " " + shortId(hd.holder);
  } else if(f.type === "G"){
    s = "grant " + h(hd.seqName) + " (" + h(hd.start) + "," + h(hd.end) + "]";
  } else if(f.type === "N"){
    s = "notify " + h(hd.channel) + ": " + h(hd.payload);
  } else if(f.type === "S"){
    s = "SEAL era " + h(hd.ordinal) + " → " + h(hd.nextEraUrl);
  } else if(f.type === "O"){
    s = "OPEN era " + h(hd.ordinal);
  } else {
    s = "";
  }
  return '<span class="part">' + s + "</span>";
}

// --- toasts -------------------------------------------------------------
function toast(title, body){
  var t = document.createElement("div"); t.className = "toast";
  t.innerHTML = "<b>" + title + "</b><br>" + body;
  document.getElementById("toasts").appendChild(t);
  setTimeout(function(){ t.remove(); }, 6000);
}

// --- wiring -------------------------------------------------------------
document.getElementById("createForm").onsubmit = async function(e){
  e.preventDefault();
  var name = document.getElementById("createName").value.trim();
  if(!name) return;
  await api("/v1/db", { method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ name: name }) });
  document.getElementById("createName").value = "";
  await refreshList();
};
document.getElementById("dbList").addEventListener("click", async function(e){
  var fork = e.target.getAttribute("data-fork");
  if(fork){
    e.stopPropagation();
    var name = prompt("Fork name?");
    if(name){ await api("/v1/db/" + fork + "/fork", { method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name: name }) }); await refreshList(); }
    return;
  }
  var item = e.target.closest(".db-item");
  if(item){ select(item.getAttribute("data-id")); }
});

loadConn();
refreshList();
setInterval(refreshList, 5000);
</script>
</body>
</html>`

/**
 * The decoder source embedded verbatim into the page script. Kept as a
 * function-body string so the browser gets an identical implementation of
 * `decodeStreamBytes` without any module loading. Logic mirrors the exported
 * TypeScript above (the test suite proves the TS copy's codec parity).
 */
function decoderSource(): string {
  return `
function decodeStreamBytes(bytes){
  var view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  var dec = new TextDecoder();
  var groups = [];
  var pos = 0;
  function frameAt(p){
    if(p + 5 > bytes.length) return null;
    var type = String.fromCharCode(bytes[p]);
    var payloadLen = view.getUint32(p + 1, false);
    var payloadStart = p + 5;
    var payloadEnd = payloadStart + payloadLen;
    if(payloadEnd > bytes.length) return null;
    var header, walBytes = 0;
    if(type === "W"){
      var hdrLen = view.getUint16(payloadStart, false);
      var headerStart = payloadStart + 2;
      var headerEnd = headerStart + hdrLen;
      header = JSON.parse(dec.decode(bytes.subarray(headerStart, headerEnd)));
      walBytes = payloadEnd - headerEnd;
    } else {
      header = JSON.parse(dec.decode(bytes.subarray(payloadStart, payloadEnd)));
    }
    return { frame: { type: type, header: header, walBytes: walBytes }, next: payloadEnd };
  }
  for(;;){
    var first = frameAt(pos);
    if(!first) break;
    var expected = first.frame.header.expectedOffset;
    var frames = [first.frame];
    var end = first.next;
    var complete = false;
    for(;;){
      var nxt = frameAt(end);
      if(!nxt){ complete = end === bytes.length; break; }
      if(nxt.frame.header.expectedOffset !== expected){ complete = true; break; }
      frames.push(nxt.frame);
      end = nxt.next;
    }
    if(!complete) break;
    groups.push({ offset: String(expected == null ? "" : expected), frames: frames });
    pos = end;
  }
  return { groups: groups, consumed: pos };
}`
}
