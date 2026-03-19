/* ═══════════════════════════════════════════════════════════════════
   PyBackup Dashboard — app.js
   Vanilla JS SPA: routing, charts, REST API, theme toggle
   ═══════════════════════════════════════════════════════════════════ */

'use strict';

// ── API ─────────────────────────────────────────────────────────────
const API = {
  async get(path) {
    const r = await fetch(`/api${path}`);
    if (!r.ok) throw new Error(`API ${path} → ${r.status}`);
    return r.json();
  },
  async post(path, body) {
    const r = await fetch(`/api${path}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!r.ok) throw new Error(`API POST ${path} → ${r.status}`);
    return r.json();
  },
  async del(path) {
    const r = await fetch(`/api${path}`, { method: 'DELETE' });
    if (!r.ok) throw new Error(`API DELETE ${path} → ${r.status}`);
    return r.json();
  },
};

// ── Toast ────────────────────────────────────────────────────────────
function toast(msg, duration = 2800) {
  const el = document.getElementById('toast');
  el.textContent = msg;
  el.classList.add('show');
  clearTimeout(el._tid);
  el._tid = setTimeout(() => el.classList.remove('show'), duration);
}

// ── Theme ────────────────────────────────────────────────────────────
const ThemeManager = {
  current: () => document.documentElement.getAttribute('data-theme') || 'dark',
  set(theme) {
    document.documentElement.setAttribute('data-theme', theme);
    localStorage.setItem('pb-theme', theme);
    document.querySelectorAll('.theme-btn').forEach(b =>
      b.classList.toggle('active', b.dataset.themeVal === theme)
    );
    // Re-render charts with new colours
    Charts.rerender();
  },
  init() {
    const saved = localStorage.getItem('pb-theme') || 'dark';
    this.set(saved);
    document.getElementById('themeToggle').addEventListener('click', () =>
      this.set(this.current() === 'dark' ? 'light' : 'dark')
    );
    document.querySelectorAll('.theme-btn').forEach(b =>
      b.addEventListener('click', () => this.set(b.dataset.themeVal))
    );
  },
};

// ── Router (view-based SPA) ──────────────────────────────────────────
const Router = {
  views: {},
  navItems: [],
  titles: {
    dashboard: 'Dashboard',
    runs: 'Backup Runs',
    engines: 'Engines',
    settings: 'Settings',
  },

  init() {
    this.navItems = document.querySelectorAll('.nav-item');
    this.navItems.forEach(a => {
      a.addEventListener('click', e => {
        e.preventDefault();
        this.navigate(a.dataset.view);
      });
    });
    document.querySelectorAll('.panel-link').forEach(a => {
      a.addEventListener('click', e => {
        e.preventDefault();
        this.navigate(a.dataset.view);
      });
    });
    // Navigate on load
    const hash = window.location.hash.replace('#', '') || 'dashboard';
    this.navigate(hash);
  },

  navigate(view) {
    if (!document.getElementById(`view-${view}`)) view = 'dashboard';

    window.location.hash = view;
    document.getElementById('pageTitle').textContent = this.titles[view] || view;

    document.querySelectorAll('.view').forEach(el =>
      el.classList.toggle('active', el.id === `view-${view}`)
    );
    this.navItems.forEach(a =>
      a.classList.toggle('active', a.dataset.view === view)
    );

    // Close sidebar on mobile
    document.getElementById('sidebar').classList.remove('open');

    Views.load(view);
  },
};

// ── State ────────────────────────────────────────────────────────────
const State = {
  runs: { data: [], total: 0, page: 0, limit: 20, job: '', status: '' },
  stats: null,
};

// ── Chart helpers ────────────────────────────────────────────────────
const Charts = {
  _activity: null,
  _engine: null,

  cssVar: name => getComputedStyle(document.documentElement).getPropertyValue(name).trim(),

  renderActivity(daily) {
    const ctx = document.getElementById('activityChart').getContext('2d');
    if (this._activity) this._activity.destroy();

    const accent   = this.cssVar('--accent');
    const green    = this.cssVar('--green');
    const text2    = this.cssVar('--text2');
    const border   = this.cssVar('--border');
    const bg2      = this.cssVar('--bg2');

    // Fill missing days
    const map = {};
    daily.forEach(d => { map[d.day] = d; });
    const labels = [], totals = [], oks = [];
    const now = new Date();
    for (let i = 29; i >= 0; i--) {
      const d = new Date(now); d.setDate(d.getDate() - i);
      const key = d.toISOString().slice(0, 10);
      labels.push(key.slice(5)); // MM-DD
      totals.push(map[key] ? map[key].total : 0);
      oks.push(map[key] ? map[key].ok : 0);
    }

    this._activity = new window.Chart(ctx, {
      type: 'bar',
      data: {
        labels,
        datasets: [
          { label: 'Success', data: oks,                  backgroundColor: green + '99', borderRadius: 3 },
          { label: 'Total',   data: totals.map((t,i) => t - oks[i]),
            backgroundColor: accent + '55', borderRadius: 3 },
        ],
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: {
          legend: { labels: { color: text2, font: { family: 'Inter', size: 12 }, boxWidth: 10 } },
          tooltip: {
            backgroundColor: bg2,
            borderColor: border, borderWidth: 1,
            titleColor: text2, bodyColor: text2,
          },
        },
        scales: {
          x: { stacked: true, ticks: { color: text2, font: { size: 10 }, maxRotation: 0, autoSkip: true, maxTicksLimit: 10 }, grid: { color: border } },
          y: { stacked: true, ticks: { color: text2, font: { size: 11 } }, grid: { color: border } },
        },
      },
    });
  },

  renderEngine(byEngine) {
    const ctx = document.getElementById('engineChart').getContext('2d');
    if (this._engine) this._engine.destroy();

    const COLORS = ['#6366f1','#22c55e','#38bdf8','#a78bfa','#eab308','#ef4444'];
    const text2  = this.cssVar('--text2');
    const bg2    = this.cssVar('--bg2');
    const border = this.cssVar('--border');

    if (!byEngine.length) return;

    this._engine = new window.Chart(ctx, {
      type: 'doughnut',
      data: {
        labels: byEngine.map(e => e.engine || 'unknown'),
        datasets: [{
          data: byEngine.map(e => e.count),
          backgroundColor: COLORS,
          borderColor: bg2,
          borderWidth: 2,
          hoverOffset: 4,
        }],
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        cutout: '68%',
        plugins: {
          legend: {
            position: 'bottom',
            labels: { color: text2, font: { family: 'Inter', size: 11 }, padding: 12, boxWidth: 10 },
          },
          tooltip: {
            backgroundColor: bg2,
            borderColor: border, borderWidth: 1,
            titleColor: text2, bodyColor: text2,
          },
        },
      },
    });
  },

  rerender() {
    if (State.stats) {
      this.renderActivity(State.stats.daily || []);
      this.renderEngine(State.stats.by_engine || []);
    }
  },
};

// ── Formatting helpers ───────────────────────────────────────────────
function fmtDate(iso) {
  if (!iso) return '—';
  const d = new Date(iso);
  return d.toLocaleDateString() + ' ' + d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
}

function fmtDuration(startIso, endIso) {
  if (!startIso || !endIso) return '—';
  const ms = new Date(endIso) - new Date(startIso);
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
  return `${Math.floor(ms / 60000)}m ${((ms % 60000) / 1000).toFixed(0)}s`;
}

function statusBadge(s) {
  const cls = { success: 'badge-success', failed: 'badge-failed', crashed: 'badge-crashed', running: 'badge-running' };
  return `<span class="badge ${cls[s] || 'badge-unknown'}">${s || 'unknown'}</span>`;
}

const ENGINE_EMOJI = { files: '📁', mongodb: '🍃', postgresql: '🐘', mysql: '🐬', mssql: '🪟', manual: '🔧' };
const ENGINE_COLOR = {
  files: 'var(--yellow-bg)', mongodb: 'var(--green-bg)',
  postgresql: 'var(--blue-bg)', mysql: 'var(--blue-bg)', mssql: 'var(--purple-bg)', manual: 'var(--bg4)',
};

// ── Views ────────────────────────────────────────────────────────────
const Views = {
  async load(view) {
    const loaders = {
      dashboard: () => this.loadDashboard(),
      runs:      () => this.loadRuns(),
      engines:   () => this.loadEngines(),
      settings:  () => this.loadSettings(),
    };
    if (loaders[view]) await loaders[view]();
  },

  // ── Dashboard ────────────────────────────────────────────────────
  async loadDashboard() {
    try {
      const data = await API.get('/stats');
      State.stats = data;

      document.getElementById('valTotal').textContent   = data.total ?? '—';
      document.getElementById('valSuccess').textContent = data.success ?? '—';
      document.getElementById('valFailed').textContent  = data.failed ?? '—';
      document.getElementById('valRate').textContent    = `${data.success_rate ?? 0}%`;

      // Recent table
      const tbody = document.getElementById('recentBody');
      if (!data.recent || !data.recent.length) {
        tbody.innerHTML = `<tr><td colspan="5"><div class="empty-state"><p>No backup runs yet.</p></div></td></tr>`;
      } else {
        tbody.innerHTML = data.recent.map(r => `
          <tr>
            <td><span class="job-name">${esc(r.job_name)}</span></td>
            <td><span class="engine-tag">${esc(r.engine)}</span></td>
            <td>${statusBadge(r.status)}</td>
            <td>${fmtDate(r.started_at)}</td>
            <td>${fmtDuration(r.started_at, r.finished_at)}</td>
          </tr>`).join('');
      }

      // Load Chart.js lazily
      await this._loadChartJs();
      Charts.renderActivity(data.daily || []);
      Charts.renderEngine(data.by_engine || []);

    } catch (err) {
      console.error('Dashboard load error:', err);
      toast('⚠ Failed to load dashboard data');
    }
  },

  // ── Runs ─────────────────────────────────────────────────────────
  async loadRuns(page = 0) {
    State.runs.page = page;
    const { limit, job, status } = State.runs;
    const qs = new URLSearchParams({
      limit, offset: page * limit,
      ...(job    ? { job }    : {}),
      ...(status ? { status } : {}),
    });

    try {
      const data = await API.get(`/runs?${qs}`);
      State.runs.data  = data.runs;
      State.runs.total = data.total;

      const tbody = document.getElementById('runsBody');
      if (!data.runs.length) {
        tbody.innerHTML = `<tr><td colspan="8"><div class="empty-state"><p>No backup runs match your filter.</p></div></td></tr>`;
      } else {
        tbody.innerHTML = data.runs.map(r => `
          <tr>
            <td style="color:var(--text3);font-size:.8rem">#${r.id}</td>
            <td><span class="job-name">${esc(r.job_name)}</span></td>
            <td><span class="engine-tag">${esc(r.engine)}</span></td>
            <td>${statusBadge(r.status)}</td>
            <td>${fmtDate(r.started_at)}</td>
            <td>${fmtDuration(r.started_at, r.finished_at)}</td>
            <td><span class="output-path" title="${esc(r.output_path || '')}">${esc(r.output_path || '—')}</span></td>
            <td style="display:flex;gap:6px">
              <button class="btn-icon btn-icon-blue" title="Details" onclick="Modal.show(${r.id})">
                <svg viewBox="0 0 16 16" fill="none"><circle cx="8" cy="8" r="6" stroke="currentColor" stroke-width="1.3"/><path d="M8 7v4M8 5.5v.5" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/></svg>
              </button>
              <button class="btn-icon btn-icon-red" title="Delete" onclick="Runs.deleteRun(${r.id})">
                <svg viewBox="0 0 16 16" fill="none"><path d="M3 4h10M6 4V3h4v1M5 4v8h6V4H5z" stroke="currentColor" stroke-width="1.3" stroke-linecap="round" stroke-linejoin="round"/></svg>
              </button>
            </td>
          </tr>`).join('');
      }

      // Pagination
      this._renderPagination(data.total, limit, page);

    } catch (err) {
      console.error('Runs load error:', err);
      toast('⚠ Failed to load runs');
    }
  },

  _renderPagination(total, limit, currentPage) {
    const pages = Math.ceil(total / limit);
    const el = document.getElementById('runsPagination');
    if (pages <= 1) { el.innerHTML = ''; return; }

    let html = `<span>${total} total</span>&nbsp;`;
    for (let i = 0; i < pages; i++) {
      html += `<button class="page-btn ${i === currentPage ? 'active' : ''}" onclick="Views.loadRuns(${i})">${i + 1}</button>`;
    }
    el.innerHTML = html;
  },

  // ── Engines ──────────────────────────────────────────────────────
  async loadEngines() {
    try {
      const data = await API.get('/stats');
      const grid = document.getElementById('enginesGrid');

      const engines = ['files','mongodb','postgresql','mysql','mssql'];
      const byEngine = {};
      (data.by_engine || []).forEach(e => { byEngine[e.engine] = e; });

      grid.innerHTML = engines.map(name => {
        const info = byEngine[name] || { count: 0, successes: 0 };
        const rate = info.count ? Math.round(info.successes / info.count * 100) : 0;
        const emoji = ENGINE_EMOJI[name] || '🔧';
        const color = ENGINE_COLOR[name] || 'var(--bg4)';
        return `
          <div class="engine-card">
            <div class="engine-card-header">
              <div class="engine-icon" style="background:${color};font-size:1.3rem">${emoji}</div>
              <div>
                <div class="engine-card-name">${name.charAt(0).toUpperCase() + name.slice(1)}</div>
                <div class="engine-card-sub">${engineDesc(name)}</div>
              </div>
            </div>
            <div class="engine-stats">
              <div class="engine-stat"><div class="engine-stat-val">${info.count}</div><div class="engine-stat-key">Runs</div></div>
              <div class="engine-stat"><div class="engine-stat-val">${info.successes || 0}</div><div class="engine-stat-key">Success</div></div>
              <div class="engine-stat"><div class="engine-stat-val">${rate}%</div><div class="engine-stat-key">Rate</div></div>
            </div>
          </div>`;
      }).join('');
    } catch (err) {
      console.error('Engines load error:', err);
      toast('⚠ Failed to load engine stats');
    }
  },

  // ── Settings ─────────────────────────────────────────────────────
  async loadSettings() {
    try {
      const data = await API.get('/settings');
      if (data.retention_days) document.getElementById('retentionDays').value = data.retention_days;
      if (data.log_level)      document.getElementById('logLevel').value      = data.log_level;
    } catch (err) {
      console.error('Settings load error:', err);
    }
  },

  // ── Chart.js lazy loader ─────────────────────────────────────────
  _chartJsLoaded: false,
  _loadChartJs() {
    if (this._chartJsLoaded || window.Chart) {
      this._chartJsLoaded = true; return Promise.resolve();
    }
    return new Promise((res, rej) => {
      const s = document.createElement('script');
      s.src = 'https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js';
      s.onload = () => { this._chartJsLoaded = true; res(); };
      s.onerror = rej;
      document.head.appendChild(s);
    });
  },
};

// ── Runs actions ─────────────────────────────────────────────────────
const Runs = {
  async deleteRun(id) {
    if (!confirm(`Delete run #${id}? This cannot be undone.`)) return;
    try {
      await API.del(`/runs/${id}`);
      toast(`Run #${id} deleted`);
      Views.loadRuns(State.runs.page);
    } catch (err) {
      toast(`⚠ Failed to delete run #${id}`);
    }
  },
};

// ── Modal ────────────────────────────────────────────────────────────
const Modal = {
  async show(runId) {
    try {
      const run = await API.get(`/runs/${runId}`);
      document.getElementById('modalTitle').textContent = `Run #${runId} — ${run.job_name}`;

      const errorHtml = run.error
        ? `<div class="error-block">${esc(run.error)}</div>` : '';

      document.getElementById('modalBody').innerHTML = `
        <div class="detail-grid">
          <div class="detail-item"><div class="detail-key">Status</div><div class="detail-val">${statusBadge(run.status)}</div></div>
          <div class="detail-item"><div class="detail-key">Engine</div><div class="detail-val">${esc(run.engine)}</div></div>
          <div class="detail-item"><div class="detail-key">Started</div><div class="detail-val">${fmtDate(run.started_at)}</div></div>
          <div class="detail-item"><div class="detail-key">Duration</div><div class="detail-val">${fmtDuration(run.started_at, run.finished_at)}</div></div>
          <div class="detail-item" style="grid-column:1/-1"><div class="detail-key">Output Path</div><div class="detail-val" style="font-family:monospace;font-size:.8rem">${esc(run.output_path || '—')}</div></div>
        </div>
        ${errorHtml}
        ${run.files && run.files.length ? this._filesHtml(run.files) : ''}
      `;

      document.getElementById('modalOverlay').classList.add('open');
    } catch (err) {
      toast('⚠ Failed to load run details');
    }
  },
  _filesHtml(files) {
    return `
      <div style="margin-top:16px;font-family:'Space Grotesk',sans-serif;font-size:.8rem;font-weight:600;color:var(--text3);text-transform:uppercase;letter-spacing:.06em;margin-bottom:8px">Files</div>
      ${files.map(f => `<div class="detail-item" style="margin-bottom:6px">
        <div class="detail-key">${esc(f.file_path)}</div>
        <div class="detail-val" style="font-size:.8rem">${f.file_size ? (f.file_size / 1024).toFixed(1) + ' KB' : '—'}</div>
      </div>`).join('')}`;
  },
  close() {
    document.getElementById('modalOverlay').classList.remove('open');
  },
};

// ── Escape HTML ──────────────────────────────────────────────────────
function esc(str) {
  return String(str || '').replace(/[&<>"']/g, c =>
    ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c])
  );
}

function engineDesc(name) {
  return { files: 'Filesystem & configs', mongodb: 'mongodump utility', postgresql: 'pg_dump utility', mysql: 'mysqldump utility', mssql: 'sqlcmd BACKUP DATABASE' }[name] || '';
}

// ── Init ─────────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  ThemeManager.init();
  Router.init();

  // Mobile sidebar toggle
  document.getElementById('menuToggle').addEventListener('click', () =>
    document.getElementById('sidebar').classList.toggle('open')
  );

  // Refresh button
  document.getElementById('refreshBtn').addEventListener('click', () => {
    const active = document.querySelector('.view.active')?.id?.replace('view-', '') || 'dashboard';
    Views.load(active);
    toast('Refreshed');
  });

  // Runs filter
  document.getElementById('runSearch').addEventListener('input', debounce(e => {
    State.runs.job = e.target.value.trim();
    Views.loadRuns(0);
  }, 350));

  document.getElementById('runStatusFilter').addEventListener('change', e => {
    State.runs.status = e.target.value;
    Views.loadRuns(0);
  });

  // Add test run
  document.getElementById('addTestRunBtn').addEventListener('click', async () => {
    const engines = ['files','mongodb','postgresql','mysql','mssql'];
    const statuses = ['success','success','success','failed','crashed'];
    const engine = engines[Math.floor(Math.random() * engines.length)];
    const status = statuses[Math.floor(Math.random() * statuses.length)];
    try {
      await API.post('/runs', {
        job_name: `test-${engine}-${Date.now() % 10000}`,
        engine, status,
        output_path: status === 'success' ? `/backups/${engine}/20240101_120000` : null,
        error: status !== 'success' ? 'Simulated error for testing' : null,
      });
      toast(`Test run added (${engine}/${status})`);
      Views.loadRuns(State.runs.page);
    } catch (err) {
      toast('⚠ Failed to add test run');
    }
  });

  // Save settings
  document.getElementById('saveSettingsBtn').addEventListener('click', async () => {
    const body = {
      retention_days: document.getElementById('retentionDays').value,
      log_level:      document.getElementById('logLevel').value,
      theme:          ThemeManager.current(),
    };
    try {
      await API.post('/settings', body);
      const status = document.getElementById('saveStatus');
      status.textContent = '✓ Saved';
      setTimeout(() => { status.textContent = ''; }, 2500);
      toast('Settings saved');
    } catch {
      toast('⚠ Failed to save settings');
    }
  });

  // Modal close
  document.getElementById('modalClose').addEventListener('click', () => Modal.close());
  document.getElementById('modalOverlay').addEventListener('click', e => {
    if (e.target === document.getElementById('modalOverlay')) Modal.close();
  });
  document.addEventListener('keydown', e => { if (e.key === 'Escape') Modal.close(); });

  // Expose to onclick handlers
  window.Modal = Modal;
  window.Runs  = Runs;
  window.Views = Views;

  // Server health pulse
  setInterval(async () => {
    try {
      await fetch('/api/stats');
      document.getElementById('statusDot').style.background = 'var(--green)';
    } catch {
      document.getElementById('statusDot').style.background = 'var(--red)';
    }
  }, 30000);
});

function debounce(fn, ms) {
  let t;
  return (...args) => { clearTimeout(t); t = setTimeout(() => fn(...args), ms); };
}
