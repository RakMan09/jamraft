// JamRaft party UI. Talks to the local node's HTTP API; the node forwards
// writes to the current leader, so the browser can point at any node.

const clientId = "web-" + Math.random().toString(36).slice(2, 10);
let seq = 0;
const nextSeq = () => ++seq;

// Peers to poll for the cluster view. Populated from /api/status if available,
// otherwise defaults to a few common local ports.
let PEERS = [];

async function api(path, method = "GET", body = null) {
  const opts = { method, headers: { "Content-Type": "application/json" } };
  if (body) opts.body = JSON.stringify(body);
  const res = await fetch(path, opts);
  if (!res.ok) throw new Error("HTTP " + res.status);
  return res.json();
}

async function refreshQueue() {
  try {
    const view = await api("/api/queue");
    renderNowPlaying(view);
    renderQueue(view.queue || []);
    setHealth(true);
  } catch (e) {
    setHealth(false);
  }
}

async function refreshStatus() {
  try {
    const st = await api("/api/status");
    renderSelfStatus(st);
  } catch (e) {
    // ignore
  }
}

function setHealth(ok) {
  const dot = document.getElementById("health-dot");
  dot.className = "dot " + (ok ? "ok" : "bad");
  document.getElementById("cluster-summary").textContent = ok
    ? "Cluster online"
    : "Waiting for a leader…";
}

function renderNowPlaying(view) {
  const np = document.getElementById("now-playing");
  if (view.nowPlaying) {
    np.textContent = view.nowPlaying.title +
      (view.nowPlaying.addedBy ? "  ·  added by " + view.nowPlaying.addedBy : "");
  } else {
    np.textContent = "Nothing playing";
  }
  const sc = document.getElementById("skip-count");
  sc.textContent = view.skipVotes ? "(" + view.skipVotes + ")" : "";
}

function renderQueue(queue) {
  const list = document.getElementById("queue-list");
  document.getElementById("queue-count").textContent = "(" + queue.length + ")";
  list.innerHTML = "";
  queue.forEach((song, i) => {
    const li = document.createElement("li");
    const left = document.createElement("div");
    left.innerHTML = `<span class="idx">${i + 1}</span> ${escapeHtml(song.title)}`;
    const right = document.createElement("div");
    right.className = "added-by";
    right.textContent = song.addedBy || "";
    li.appendChild(left);
    li.appendChild(right);
    list.appendChild(li);
  });
}

function renderSelfStatus(st) {
  const nodes = document.getElementById("nodes");
  nodes.innerHTML = "";
  const card = document.createElement("div");
  card.className = "node" + (st.role === "leader" ? " leader" : "");
  card.innerHTML = `
    <div><strong>${st.id}</strong> <span class="role ${st.role}">${st.role}</span></div>
    <div class="meta">term ${st.term} · commit ${st.commitIndex} · log ${st.logSize}</div>
    <div class="meta">leader: ${st.leaderId || "—"}</div>
  `;
  const kill = document.createElement("button");
  kill.className = "kill";
  kill.textContent = "Kill this node";
  kill.onclick = async () => {
    if (!confirm("Kill this node? Playback should continue via a new leader.")) return;
    try { await api("/api/admin/kill", "POST", {}); } catch (e) {}
  };
  card.appendChild(kill);
  nodes.appendChild(card);
}

function escapeHtml(s) {
  const d = document.createElement("div");
  d.textContent = s;
  return d.innerHTML;
}

document.getElementById("add-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const title = document.getElementById("song-title").value.trim();
  const addedBy = document.getElementById("added-by").value.trim();
  if (!title) return;
  await api("/api/enqueue", "POST", { title, addedBy, clientId, seq: nextSeq() });
  document.getElementById("song-title").value = "";
  refreshQueue();
});

document.getElementById("btn-play-next").addEventListener("click", async () => {
  await api("/api/play-next", "POST", { clientId, seq: nextSeq() });
  refreshQueue();
});

document.getElementById("btn-vote-skip").addEventListener("click", async () => {
  await api("/api/vote-skip", "POST", { voter: clientId, clientId, seq: nextSeq() });
  refreshQueue();
});

setInterval(refreshQueue, 1000);
setInterval(refreshStatus, 1000);
refreshQueue();
refreshStatus();
