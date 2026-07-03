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

function renderNodes(s) {
  const wrap = document.getElementById("nodes");
  wrap.innerHTML = "";
  for (const n of s.nodes) {
    const card = document.createElement("div");
    card.className = "node" + (n.leader ? " leader" : "") + (n.down ? " down" : "") + (n.isolated ? " isolated" : "");
    const role = n.down ? "down" : n.role;
    card.innerHTML = `
      <div class="name">${n.id} <span class="badge ${role}">${role.toUpperCase()}</span></div>
      <div class="meta">
        term <b>${n.term}</b> · commit <b>${n.commit}</b><br/>
        log ${n.logSize} · applied ${n.applied}${n.snapshotIndex ? " · snap@" + n.snapshotIndex : ""}
      </div>`;
    const actions = document.createElement("div");
    actions.className = "actions";
    if (n.down) {
      actions.appendChild(btn("Restart", "tiny secondary", () => { window.jrRestart(n.id); render(); }));
    } else {
      actions.appendChild(btn("Kill", "tiny kill", () => { window.jrKill(n.id); render(); }));
      const iso = btn(n.isolated ? "Rejoin" : "Isolate", "tiny iso" + (n.isolated ? " active" : ""), () => { window.jrToggleIsolate(n.id); render(); });
      actions.appendChild(iso);
    }
    card.appendChild(actions);
    wrap.appendChild(card);
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

function btn(label, cls, onclick) {
  const b = document.createElement("button");
  b.className = cls;
  b.textContent = label;
  b.onclick = onclick;
  return b;
}

function escapeHtml(str) {
  const d = document.createElement("div");
  d.textContent = str;
  return d.innerHTML;
}

boot();
