#!/usr/bin/env node
/**
 * Kaggle Node Log Streamer — Blessed TUI
 *
 * 3 ta split panelda har bir Kaggle node log'ini real-time ko'rsatadi.
 * Log'lar aralashmaydi, har panel mustaqil scroll qiladi.
 *
 * Ishlatish:
 *   node scripts/log_stream.mjs              # barcha 3 node
 *   node scripts/log_stream.mjs --node 0     # faqat Node-0
 *   node scripts/log_stream.mjs --node 0,2   # Node-0 + 2
 *
 * Klavishlar:
 *   1/2/3      — Node paneliga fokus
 *   Tab        — Keyingi panel
 *   Yuqori/Past — Scroll
 *   PageUp/Dn  — Sahifa bo'ylab scroll
 *   Home/End   — Boshiga/oxiriga o'tish
 *   q / Ctrl+C — Chiqish
 */

import blessed from 'blessed';
import dotenv from 'dotenv';
import { spawn } from 'child_process';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';
import { readFileSync } from 'fs';

const __dirname = dirname(fileURLToPath(import.meta.url));

// ============================================================
// Config
// ============================================================

dotenv.config({ path: resolve(__dirname, '..', '.env') });

const NODES = {
  0: {
    label: 'Node-0 (LLM+TTS UZ)',
    kernel: 'bunyodbek7/ai-operator-kaggle-node',
    color: 'green',
    userEnv: 'KAGGLE_USERNAME',
    keyEnv: 'KAGGLE_KEY',
  },
  1: {
    label: 'Node-1 (STT RU+TTS)',
    kernel: 'bunyodozodboyev/ai-operator-kaggle-node-1',
    color: 'cyan',
    userEnv: 'KAGGLE_USERNAME_1',
    keyEnv: 'KAGGLE_KEY_1',
  },
  2: {
    label: 'Node-2 (STT EN+UZ)',
    kernel: 'bunyodbekozodboyev/ai-operator-kaggle-node-2',
    color: 'magenta',
    userEnv: 'KAGGLE_USERNAME_2',
    keyEnv: 'KAGGLE_KEY_2',
  },
};

// ============================================================
// Helpers
// ============================================================

function getAuth(nodeIdx) {
  const cfg = NODES[nodeIdx];
  const user = process.env[cfg.userEnv] || '';
  const key = process.env[cfg.keyEnv] || '';
  return { user, key };
}

function makeEnv(nodeIdx) {
  const { user, key } = getAuth(nodeIdx);
  const env = { ...process.env };
  env.KAGGLE_USERNAME = user;
  env.KAGGLE_KEY = key;
  // Remove global KAGGLE_API_TOKEN to avoid cross-account auth
  delete env.KAGGLE_API_TOKEN;
  return env;
}

function parseArgs() {
  const args = process.argv.slice(2);
  let selected = [0, 1, 2];

  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--node' && args[i + 1]) {
      const val = args[i + 1].toLowerCase();
      if (val === 'all') {
        selected = [0, 1, 2];
      } else if (val.includes(',')) {
        selected = val.split(',').map(s => parseInt(s.trim())).filter(n => [0, 1, 2].includes(n));
      } else {
        const n = parseInt(val);
        if ([0, 1, 2].includes(n)) selected = [n];
      }
      i++;
    }
  }
  return [...new Set(selected)].sort();
}

// ============================================================
// TUI
// ============================================================

function createTUI(nodes) {
  const screen = blessed.screen({
    smartCSR: true,
    title: 'Kaggle Node Log Streamer',
    dockBorders: true,
    fullUnicode: true,
  });

  // Help bar at the bottom
  const helpBar = blessed.box({
    bottom: 0,
    left: 0,
    width: '100%',
    height: 1,
    content: ' 1/2/3: fokus | ↑↓: scroll | Tab: keyingi | q: chiqish',
    style: {
      fg: 'black',
      bg: 'white',
      bold: true,
    },
  });

  // Create a panel for each node
  const panels = {};
  const streams = {};
  const logLines = {};
  let focusedIdx = nodes[0];

  nodes.forEach((n, i) => {
    const cfg = NODES[n];
    logLines[n] = [];

    // Calculate position — stack vertically
    const height = `${Math.floor(100 / nodes.length)}%-1`;

    const panel = blessed.log({
      top: i === 0 ? 0 : `${Math.floor(i * 100 / nodes.length)}%`,
      left: 0,
      width: '100%',
      height: height,
      border: { type: 'line' },
      label: ` ${cfg.label} `,
      tags: true,
      scrollable: true,
      alwaysScroll: true,
      scrollbar: { ch: ' ', track: { bg: 'grey' }, style: { inverse: true } },
      mouse: true,
      keys: true,
      vi: true,
      style: {
        border: { fg: cfg.color },
        label: { fg: cfg.color, bold: true },
        scrollbar: { bg: cfg.color },
      },
    });

    // Initial content
    const { user } = getAuth(n);
    panel.log(`🔌 Ulanilmoqda... (kernel: ${cfg.kernel})`);
    panel.log(`   Akkaunt: ${user}`);
    panel.log('');

    panels[n] = panel;
    screen.append(panel);
  });

  screen.append(helpBar);

  // Focus first panel
  if (panels[focusedIdx]) {
    panels[focusedIdx].focus();
    panels[focusedIdx].style.border = { fg: NODES[focusedIdx].color, bold: true };
  }

  // ============================================================
  // Key bindings
  // ============================================================

  screen.key(['1', '2', '3'], (ch) => {
    const n = parseInt(ch);
    if (panels[n]) {
      focusedIdx = n;
      // Reset all borders
      nodes.forEach(x => {
        panels[x].style.border = { fg: NODES[x].color };
      });
      panels[n].focus();
      panels[n].style.border = { fg: NODES[n].color, bold: true };
      screen.render();
    }
  });

  screen.key(['tab'], () => {
    const idx = nodes.indexOf(focusedIdx);
    const nextIdx = (idx + 1) % nodes.length;
    focusedIdx = nodes[nextIdx];
    nodes.forEach(x => {
      panels[x].style.border = { fg: NODES[x].color };
    });
    panels[focusedIdx].focus();
    panels[focusedIdx].style.border = { fg: NODES[focusedIdx].color, bold: true };
    screen.render();
  });

  screen.key(['q', 'C-c'], () => {
    cleanup();
    process.exit(0);
  });

  // ============================================================
  // Stream spawners
  // ============================================================

  nodes.forEach(n => {
    const cfg = NODES[n];
    const { user, key } = getAuth(n);

    if (!key) {
      panels[n].log(`⚠️  Kaggle kaliti topilmadi (${cfg.keyEnv})`);
      screen.render();
      return;
    }

    const env = makeEnv(n);
    const child = spawn('kaggle', ['kernels', 'logs', cfg.kernel, '-f'], {
      env,
      stdio: ['ignore', 'pipe', 'pipe'],
    });

    streams[n] = child;

    child.stdout.on('data', (data) => {
      const lines = data.toString().split('\n').filter(l => l.trim());
      lines.forEach(line => {
        panels[n].log(line);
        logLines[n].push(line);
        // Keep only last 1000 lines in memory
        if (logLines[n].length > 1000) logLines[n].shift();
      });
      screen.render();
    });

    child.stderr.on('data', (data) => {
      panels[n].log(`⚠️  ${data.toString().trim()}`);
      screen.render();
    });

    child.on('close', (code) => {
      panels[n].log(`── Stream uzildi (exit=${code}) ──`);
      screen.render();
    });

    child.on('error', (err) => {
      panels[n].log(`❌ Xatolik: ${err.message}`);
      screen.render();
    });
  });

  // ============================================================
  // Cleanup
  // ============================================================

  function cleanup() {
    nodes.forEach(n => {
      if (streams[n]) {
        streams[n].kill();
      }
    });
  }

  screen.on('destroy', cleanup);

  // ============================================================
  // Render
  // ============================================================

  screen.render();
  return screen;
}

// ============================================================
// Main
// ============================================================

function main() {
  const nodes = parseArgs();

  if (nodes.length === 0) {
    console.log('Noto\'g\'ri node raqami. 0, 1, 2, all');
    process.exit(1);
  }

  console.clear();
  createTUI(nodes);
}

main();
