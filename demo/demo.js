// Boots the JamRaft WASM cluster and renders a live dashboard. Every button
// calls into the real Go/Raft code compiled to WebAssembly.

const REPO_URL = "https://github.com/RakMan09/jamraft";

async function boot() {
  document.getElementById("repo").href = REPO_URL;
  const go = new Go();
  try {
    const result = await WebAssembly.instantiateStreaming(fetch("jamraft.wasm"), go.importObject);
    go.run(result.instance);
  } catch (e) {
    // Fallback for servers that don't set application/wasm content-type.
    const resp = await fetch("jamraft.wasm");
    const bytes = await resp.arrayBuffer();
    const result = await WebAssembly.instantiate(bytes, go.importObject);
    go.run(result.instance);
  }
  await waitReady();
  document.getElementById("boot").textContent = "Consensus running locally in this tab.";
  document.getElementById("app").hidden = false;
  wireControls();
  setInterval(render, 400);
  render();
}

function waitReady() {
  return new Promise((resolve) => {
    const check = () => (window.jamraftReady ? resolve() : setTimeout(check, 30));
    check();
  });
}

function wireControls() {
  document.getElementById("add").addEventListener("submit", (e) => {
    e.preventDefault();
    const t = document.getElementById("title");
    const by = document.getElementById("by");
    if (!t.value.trim()) return;
    window.jrEnqueue(t.value.trim(), by.value.trim());
    t.value = "";
    render();
  });
  document.getElementById("play-next").onclick = () => { window.jrPlayNext(); render(); };
  document.getElementById("vote-skip").onclick = () => { window.jrVoteSkip("guest-" + Math.floor(Math.random() * 1000)); render(); };
  document.getElementById("reset").onclick = () => {
    window.jrReset(parseInt(document.getElementById("size").value, 10));
    render();
  };
  document.getElementById("heal").onclick = () => {
    window.jrHeal();
    document.getElementById("drop").value = 0;
    document.getElementById("drop-val").textContent = "0%";
    render();
  };
  const drop = document.getElementById("drop");
  drop.oninput = () => {
    document.getElementById("drop-val").textContent = drop.value + "%";
    window.jrSetDrop(parseInt(drop.value, 10) / 100);
  };
}

function render() {
  let s;
  try {
    s = JSON.parse(window.jrState());
  } catch (e) {
    return;
  }
  if (!s.nodes) return;
  renderNodes(s);
  renderJukebox(s);
  document.getElementById("stats").textContent =
    `leader: ${s.leaderId || "—"}  ·  msgs delivered ${s.delivered} / dropped ${s.dropped}`;
}

// Node cards are created ONCE and then updated in place. Previously the whole
// list was rebuilt every render tick (400ms), which recreated the Kill/Isolate
// buttons; a click landing during a rebuild was lost because the button was
// removed between mousedown and mouseup. Persistent elements fix that.
let nodeEls = {}; // id -> { root, badge, meta, primary, iso }
let nodeSig = "";

function buildCard(id) {
  const root = document.createElement("div");
  root.className = "node";

  const name = document.createElement("div");
  name.className = "name";
  const label = document.createElement("span");
  label.textContent = id;
  const badge = document.createElement("span");
  badge.className = "badge";
  name.append(label, badge);

  const meta = document.createElement("div");
  meta.className = "meta";

  const actions = document.createElement("div");
  actions.className = "actions";
  const primary = document.createElement("button");
  const iso = document.createElement("button");
  actions.append(primary, iso);

  // Handlers are attached once; they read current state at click time, so the
  // buttons never need to be recreated.
  primary.addEventListener("click", () => {
    if (root.dataset.down === "true") window.jrRestart(id);
    else window.jrKill(id);
    render();
  });
  iso.addEventListener("click", () => {
    window.jrToggleIsolate(id);
    render();
  });

  root.append(name, meta, actions);
  return { root, badge, meta, primary, iso };
}

function renderNodes(s) {
  const wrap = document.getElementById("nodes");
  const sig = s.nodes.map((n) => n.id).join(",");
  if (sig !== nodeSig) {
    wrap.innerHTML = "";
    nodeEls = {};
    for (const n of s.nodes) {
      const els = buildCard(n.id);
      nodeEls[n.id] = els;
      wrap.appendChild(els.root);
    }
    nodeSig = sig;
  }
  for (const n of s.nodes) {
    const els = nodeEls[n.id];
    if (!els) continue;
    const role = n.down ? "down" : n.role;
    els.root.className =
      "node" + (n.leader ? " leader" : "") + (n.down ? " down" : "") + (n.isolated ? " isolated" : "");
    els.root.dataset.down = n.down ? "true" : "false";
    els.badge.className = "badge " + role;
    els.badge.textContent = role.toUpperCase();
    els.meta.innerHTML =
      `term <b>${n.term}</b> · commit <b>${n.commit}</b><br/>` +
      `log ${n.logSize} · applied ${n.applied}${n.snapshotIndex ? " · snap@" + n.snapshotIndex : ""}`;
    if (n.down) {
      els.primary.textContent = "Restart";
      els.primary.className = "tiny secondary";
      els.iso.style.display = "none";
    } else {
      els.primary.textContent = "Kill";
      els.primary.className = "tiny kill";
      els.iso.style.display = "";
      els.iso.textContent = n.isolated ? "Rejoin" : "Isolate";
      els.iso.className = "tiny iso" + (n.isolated ? " active" : "");
    }
  }
}

function renderJukebox(s) {
  const np = document.getElementById("now-playing");
  np.textContent = s.nowPlaying
    ? s.nowPlaying.title + (s.nowPlaying.addedBy ? "  ·  " + s.nowPlaying.addedBy : "")
    : "Nothing playing";
  document.getElementById("skip").textContent = s.skipVotes ? "(" + s.skipVotes + ")" : "";

  const q = s.queue || [];
  document.getElementById("qc").textContent = "(" + q.length + ")";
  const list = document.getElementById("queue");
  list.innerHTML = "";
  q.forEach((song, i) => {
    const li = document.createElement("li");
    const left = document.createElement("span");
    left.innerHTML = `<span class="idx">${i + 1}</span>${escapeHtml(song.title)}`;
    const right = document.createElement("span");
    right.className = "by";
    right.textContent = song.addedBy || "";
    li.append(left, right);
    list.appendChild(li);
  });
}

function escapeHtml(str) {
  const d = document.createElement("div");
  d.textContent = str;
  return d.innerHTML;
}

boot();
