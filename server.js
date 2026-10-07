// Live understanding meter: red / yellow / green
// No dependencies. Run with: node server.js
// Anonymous: each browser gets a random ID. No IP addresses are read or stored.

const http = require("http");

const PORT = process.env.PORT || 3000;
const HOST_KEY = process.env.HOST_KEY || "teacher"; // change this before you deploy
const COLORS = ["red", "yellow", "green"];
const STALE_MS = 5 * 60 * 1000; // drop a vote 5 minutes after the person leaves

const votes = new Map(); // clientId -> color
const lastSeen = new Map(); // clientId -> timestamp
const liveConns = new Map(); // clientId -> number of open streams
const streams = new Set(); // open SSE responses
const messages = []; // anonymous questions and comments, newest last
const lastAsk = new Map(); // clientId -> timestamp, for rate limiting
let msgSeq = 0;
const MAX_MSGS = 100;
const MAX_LEN = 300;

function sendAll(event, data) {
  const payload = `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
  for (const res of streams) res.write(payload);
}

function counts() {
  const c = { red: 0, yellow: 0, green: 0 };
  for (const color of votes.values()) c[color]++;
  return c;
}

function broadcast() {
  const payload = `data: ${JSON.stringify({ counts: counts(), watching: liveConns.size })}\n\n`;
  for (const res of streams) res.write(payload);
}

setInterval(() => {
  const now = Date.now();
  let changed = false;
  for (const [id, t] of lastSeen) {
    if (!liveConns.has(id) && now - t > STALE_MS) {
      lastSeen.delete(id);
      lastAsk.delete(id);
      if (votes.delete(id)) changed = true;
    }
  }
  if (changed) broadcast();
}, 30 * 1000);

setInterval(() => {
  for (const res of streams) res.write(": ping\n\n");
}, 20 * 1000);

const PAGE = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Understanding Meter</title>
<style>
  :root {
    --bg: #f6f5f1; --card: #ffffff; --ink: #1e1e1c; --muted: #6b6a64; --line: #e3e1da;
    --red: #d64545; --yellow: #e8b020; --green: #2f9e5b;
  }
  @media (prefers-color-scheme: dark) {
    :root { --bg: #161615; --card: #212120; --ink: #f1f0ea; --muted: #a09f97; --line: #343432; }
  }
  * { box-sizing: border-box; }
  html, body { height: 100%; }
  body {
    margin: 0; background: var(--bg); color: var(--ink);
    font-family: system-ui, -apple-system, "Segoe UI", Roboto, sans-serif;
    display: flex; flex-direction: column; min-height: 100vh;
  }
  header { padding: 20px 20px 4px; text-align: center; }
  h1 { margin: 0; font-size: 1.25rem; font-weight: 600; }
  .sub { color: var(--muted); font-size: .9rem; margin-top: 4px; }
  main { flex: 1; width: 100%; max-width: 760px; margin: 0 auto; padding: 16px 20px; }
  .results { display: grid; grid-template-columns: repeat(3, 1fr); gap: 12px; }
  .stat {
    background: var(--card); border: 1px solid var(--line); border-radius: 14px;
    padding: 18px 8px 14px; text-align: center; border-top-width: 6px;
  }
  .stat.red { border-top-color: var(--red); }
  .stat.yellow { border-top-color: var(--yellow); }
  .stat.green { border-top-color: var(--green); }
  .num { font-size: clamp(2.6rem, 11vw, 4.5rem); font-weight: 700; line-height: 1; font-variant-numeric: tabular-nums; }
  .lbl { color: var(--muted); font-size: .85rem; margin-top: 6px; }
  .bar {
    display: flex; height: 18px; border-radius: 9px; overflow: hidden;
    background: var(--line); margin: 18px 0 6px;
  }
  .bar div { transition: width .35s ease; }
  .bar .r { background: var(--red); } .bar .y { background: var(--yellow); } .bar .g { background: var(--green); }
  .total { text-align: center; color: var(--muted); font-size: .9rem; }
  footer {
    position: sticky; bottom: 0; background: var(--bg);
    border-top: 1px solid var(--line); padding: 14px 20px calc(14px + env(safe-area-inset-bottom, 0px));
  }
  .prompt { text-align: center; font-size: .9rem; color: var(--muted); margin-bottom: 10px; }
  .prompt button {
    margin-left: 8px; background: none; border: 1px solid var(--line); color: var(--muted);
    border-radius: 8px; padding: 2px 10px; font: inherit; font-size: .8rem; cursor: pointer;
  }
  .prompt button:disabled { opacity: .4; cursor: default; }
  .btns { display: grid; grid-template-columns: repeat(3, 1fr); gap: 10px; max-width: 760px; margin: 0 auto; }
  .btn {
    border: 3px solid transparent; border-radius: 14px; padding: 16px 6px; cursor: pointer;
    color: #fff; font-size: 1rem; font-weight: 600; line-height: 1.25;
    display: flex; flex-direction: column; gap: 4px; align-items: center; justify-content: center;
    opacity: .85; transition: transform .08s, opacity .15s; touch-action: manipulation; user-select: none;
  }
  .btn small { font-weight: 500; font-size: .78rem; opacity: .95; }
  .btn:active { transform: scale(.97); }
  .btn.red { background: var(--red); } .btn.yellow { background: var(--yellow); color: #2a2200; } .btn.green { background: var(--green); }
  .btn.picked { opacity: 1; border-color: var(--ink); box-shadow: 0 0 0 3px var(--bg), 0 0 0 5px var(--ink); }
  .status { text-align: center; font-size: .78rem; color: var(--muted); margin-top: 10px; min-height: 1em; }
  .qa { margin-top: 28px; }
  .qa h2 { font-size: 1rem; font-weight: 600; margin: 0 0 10px; }
  .ask { display: flex; gap: 8px; align-items: flex-end; }
  .ask textarea {
    flex: 1; resize: none; height: 64px; padding: 10px 12px; font: inherit; color: var(--ink);
    background: var(--card); border: 1px solid var(--line); border-radius: 10px;
  }
  .ask button {
    background: var(--ink); color: var(--bg); border: 0; border-radius: 10px;
    padding: 0 18px; height: 44px; font: inherit; font-weight: 600; cursor: pointer;
  }
  .ask button:disabled { opacity: .5; cursor: default; }
  .hint { color: var(--muted); font-size: .78rem; margin: 6px 2px 0; min-height: 1em; }
  .feed { list-style: none; margin: 14px 0 0; padding: 0; display: flex; flex-direction: column; gap: 8px; }
  .feed li {
    background: var(--card); border: 1px solid var(--line); border-radius: 10px;
    padding: 10px 12px; overflow-wrap: anywhere; white-space: pre-wrap;
  }
  .feed .meta { display: block; color: var(--muted); font-size: .75rem; margin-top: 4px; white-space: normal; }
  .feed .rm { background: none; border: 0; color: var(--muted); cursor: pointer; font-size: .75rem; padding: 0 0 0 8px; text-decoration: underline; }
  .empty { color: var(--muted); font-size: .9rem; text-align: center; padding: 8px 0; }
  .host { text-align: center; margin-top: 10px; display: none; }
  .host button { background: none; border: 1px solid var(--line); color: var(--muted); border-radius: 8px; padding: 6px 12px; cursor: pointer; }
</style>
</head>
<body>
<header>
  <h1>How well are you following?</h1>
  <div class="sub">Anonymous. Tap one at any time and change it whenever you like.</div>
</header>

<main>
  <div class="results">
    <div class="stat red"><div class="num" id="n-red">0</div><div class="lbl">Lost</div></div>
    <div class="stat yellow"><div class="num" id="n-yellow">0</div><div class="lbl">Kind of get it</div></div>
    <div class="stat green"><div class="num" id="n-green">0</div><div class="lbl">Fully get it</div></div>
  </div>
  <div class="bar"><div class="r" id="b-red" style="width:0"></div><div class="y" id="b-yellow" style="width:0"></div><div class="g" id="b-green" style="width:0"></div></div>
  <div class="total" id="total">No responses yet</div>

  <section class="qa">
    <h2>Questions and comments</h2>
    <div class="ask">
      <textarea id="q" maxlength="300" placeholder="Ask a question or leave a comment. It is anonymous and everyone can see it."></textarea>
      <button id="send">Send</button>
    </div>
    <div class="hint" id="hint"></div>
    <ul class="feed" id="feed"></ul>
    <div class="empty" id="empty">Nothing yet</div>
  </section>
</main>

<footer>
  <div class="prompt">Your response<button id="clear" disabled>Clear</button></div>
  <div class="btns">
    <button class="btn red" data-c="red">Red<small>I'm lost</small></button>
    <button class="btn yellow" data-c="yellow">Yellow<small>I kind of get it</small></button>
    <button class="btn green" data-c="green">Green<small>I fully get this</small></button>
  </div>
  <div class="status" id="status"></div>
  <div class="host" id="host">
    <button id="reset">Reset responses</button>
    <button id="clearq">Clear all comments</button>
  </div>
</footer>

<script>
(function () {
  var COLORS = ["red", "yellow", "green"];

  // Anonymous per-browser ID. Not tied to an IP address.
  var id;
  try { id = localStorage.getItem("um-id"); } catch (e) {}
  if (!id) {
    id = (crypto.randomUUID ? crypto.randomUUID() : String(Math.random()).slice(2) + Date.now());
    try { localStorage.setItem("um-id", id); } catch (e) {}
  }

  var mine = null;
  var statusEl = document.getElementById("status");

  function render(data) {
    var c = data.counts, total = c.red + c.yellow + c.green;
    COLORS.forEach(function (k) {
      document.getElementById("n-" + k).textContent = c[k];
      document.getElementById("b-" + k).style.width = total ? (c[k] / total * 100) + "%" : "0";
    });
    document.getElementById("total").textContent =
      total === 0 ? "No responses yet" : total + (total === 1 ? " response" : " responses");
  }

  function markPicked() {
    document.querySelectorAll(".btn").forEach(function (b) {
      b.classList.toggle("picked", b.dataset.c === mine);
    });
    document.getElementById("clear").disabled = !mine;
  }

  function clearMine() {
    if (!mine) return;
    mine = null; markPicked();
    fetch("/vote", {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ id: id, color: null })
    }).catch(function () { statusEl.textContent = "Could not send. Check your connection."; });
  }

  document.getElementById("clear").addEventListener("click", clearMine);
  document.querySelectorAll(".btn").forEach(function (b) {
    b.addEventListener("dblclick", clearMine);
  });

  var hostKey = new URLSearchParams(location.search).get("host");
  var feed = document.getElementById("feed");
  var emptyEl = document.getElementById("empty");
  var hintEl = document.getElementById("hint");

  function postJSON(path, body) {
    return fetch(path, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
  }

  function syncEmpty() { emptyEl.style.display = feed.children.length ? "none" : "block"; }

  function addMsg(m, atTop) {
    var li = document.createElement("li");
    li.dataset.n = m.n;
    li.appendChild(document.createTextNode(m.t)); // text only, never HTML
    var meta = document.createElement("span");
    meta.className = "meta";
    meta.textContent = new Date(m.at).toLocaleTimeString([], { hour: "numeric", minute: "2-digit" });
    if (hostKey) {
      var rm = document.createElement("button");
      rm.className = "rm"; rm.textContent = "remove";
      rm.addEventListener("click", function () { postJSON("/delete", { key: hostKey, n: m.n }); });
      meta.appendChild(rm);
    }
    li.appendChild(meta);
    if (atTop && feed.firstChild) feed.insertBefore(li, feed.firstChild); else feed.appendChild(li);
    syncEmpty();
  }

  function connect() {
    var es = new EventSource("/events?id=" + encodeURIComponent(id));
    es.addEventListener("msgs", function (e) {
      feed.innerHTML = "";
      JSON.parse(e.data).slice().reverse().forEach(function (m) { addMsg(m, false); });
      syncEmpty();
    });
    es.addEventListener("msg", function (e) { addMsg(JSON.parse(e.data), true); });
    es.addEventListener("del", function (e) {
      var n = String(JSON.parse(e.data));
      Array.prototype.forEach.call(feed.children, function (li) { if (li.dataset.n === n) li.remove(); });
      syncEmpty();
    });
    es.addEventListener("clear", function () { feed.innerHTML = ""; syncEmpty(); });
    es.onmessage = function (e) {
      var d = JSON.parse(e.data);
      if (d.mine !== undefined) { mine = d.mine; markPicked(); }
      if (d.counts) render(d);
      statusEl.textContent = "";
    };
    es.onerror = function () { statusEl.textContent = "Reconnecting..."; };
  }

  document.querySelectorAll(".btn").forEach(function (b) {
    b.addEventListener("click", function () {
      mine = b.dataset.c; markPicked();
      fetch("/vote", {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ id: id, color: mine })
      }).catch(function () { statusEl.textContent = "Could not send. Check your connection."; });
    });
  });

  var sendBtn = document.getElementById("send");
  var qEl = document.getElementById("q");
  function sendQ() {
    var text = qEl.value.trim();
    if (!text) return;
    sendBtn.disabled = true;
    postJSON("/ask", { id: id, text: text }).then(function (r) {
      if (r.ok) { qEl.value = ""; hintEl.textContent = ""; }
      else if (r.status === 429) hintEl.textContent = "Please wait a few seconds before sending another.";
      else hintEl.textContent = "Could not send.";
    }).catch(function () { hintEl.textContent = "Could not send. Check your connection."; })
      .then(function () { setTimeout(function () { sendBtn.disabled = false; }, 1500); });
  }
  sendBtn.addEventListener("click", sendQ);
  qEl.addEventListener("keydown", function (e) {
    if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); sendQ(); }
  });

  if (hostKey) {
    document.getElementById("host").style.display = "block";
    document.getElementById("clearq").addEventListener("click", function () {
      if (confirm("Clear all comments for everyone?")) postJSON("/clearmsgs", { key: hostKey });
    });
    document.getElementById("reset").addEventListener("click", function () {
      if (!confirm("Clear everyone's responses?")) return;
      fetch("/reset", {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ key: hostKey })
      }).then(function (r) { if (!r.ok) statusEl.textContent = "Wrong host key."; });
    });
  }

  connect();
})();
</script>
</body>
</html>`;

function readBody(req, cb) {
  let body = "";
  req.on("data", (d) => {
    body += d;
    if (body.length > 2000) req.destroy();
  });
  req.on("end", () => {
    try { cb(JSON.parse(body || "{}")); } catch (e) { cb(null); }
  });
}

const validId = (s) => typeof s === "string" && s.length >= 8 && s.length <= 80;

const server = http.createServer((req, res) => {
  const url = new URL(req.url, "http://x");

  if (req.method === "GET" && url.pathname === "/") {
    res.writeHead(200, { "Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-store" });
    return res.end(PAGE);
  }

  if (req.method === "GET" && url.pathname === "/events") {
    const id = url.searchParams.get("id");
    res.writeHead(200, {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache, no-transform",
      Connection: "keep-alive",
      "X-Accel-Buffering": "no",
    });
    streams.add(res);
    if (validId(id)) {
      liveConns.set(id, (liveConns.get(id) || 0) + 1);
      lastSeen.set(id, Date.now());
    }
    res.write(`data: ${JSON.stringify({ counts: counts(), mine: validId(id) ? votes.get(id) || null : null })}\n\n`);
    res.write(`event: msgs\ndata: ${JSON.stringify(messages)}\n\n`);
    req.on("close", () => {
      streams.delete(res);
      if (validId(id)) {
        const n = (liveConns.get(id) || 1) - 1;
        if (n <= 0) liveConns.delete(id); else liveConns.set(id, n);
        lastSeen.set(id, Date.now());
      }
    });
    return;
  }

  if (req.method === "POST" && url.pathname === "/vote") {
    return readBody(req, (b) => {
      if (!b || !validId(b.id) || !(b.color === null || COLORS.includes(b.color))) { res.writeHead(400); return res.end(); }
      if (b.color === null) votes.delete(b.id); else votes.set(b.id, b.color);
      lastSeen.set(b.id, Date.now());
      broadcast();
      res.writeHead(204); res.end();
    });
  }

  if (req.method === "POST" && url.pathname === "/ask") {
    return readBody(req, (b) => {
      if (!b || !validId(b.id) || typeof b.text !== "string") { res.writeHead(400); return res.end(); }
      const text = b.text.replace(/[\u0000-\u0008\u000b\u000c\u000e-\u001f]/g, "").trim().slice(0, MAX_LEN);
      if (!text) { res.writeHead(400); return res.end(); }
      const now = Date.now();
      if (now - (lastAsk.get(b.id) || 0) < 4000) { res.writeHead(429); return res.end(); }
      lastAsk.set(b.id, now);
      const m = { n: ++msgSeq, t: text, at: now };
      messages.push(m);
      if (messages.length > MAX_MSGS) messages.shift();
      sendAll("msg", m);
      res.writeHead(204); res.end();
    });
  }

  if (req.method === "POST" && url.pathname === "/delete") {
    return readBody(req, (b) => {
      if (!b || b.key !== HOST_KEY) { res.writeHead(403); return res.end(); }
      const i = messages.findIndex((m) => m.n === b.n);
      if (i >= 0) { messages.splice(i, 1); sendAll("del", b.n); }
      res.writeHead(204); res.end();
    });
  }

  if (req.method === "POST" && url.pathname === "/clearmsgs") {
    return readBody(req, (b) => {
      if (!b || b.key !== HOST_KEY) { res.writeHead(403); return res.end(); }
      messages.length = 0;
      sendAll("clear", true);
      res.writeHead(204); res.end();
    });
  }

  if (req.method === "POST" && url.pathname === "/reset") {
    return readBody(req, (b) => {
      if (!b || b.key !== HOST_KEY) { res.writeHead(403); return res.end(); }
      votes.clear();
      broadcast();
      // Tell each client its own pick was cleared
      for (const s of streams) s.write(`data: ${JSON.stringify({ counts: counts(), mine: null })}\n\n`);
      res.writeHead(204); res.end();
    });
  }

  res.writeHead(404); res.end("Not found");
});

server.listen(PORT, () => console.log("Understanding meter running on port " + PORT));
