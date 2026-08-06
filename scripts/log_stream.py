#!/usr/bin/env python3
"""
Kaggle Node Log Streamer — Real-time log streaming.

Har bir node log'ini alohida rangda va [Node-X] prefiks bilan ko'rsatadi.
Bir nechta node'ni bir vaqtda kuzatish mumkin.

Ishlatish:
    python3 scripts/log_stream.py              # interaktiv menyu
    python3 scripts/log_stream.py --node 0     # faqat Node-0
    python3 scripts/log_stream.py --node all   # barcha node'lar
    python3 scripts/log_stream.py --node 0,2   # Node-0 va Node-2

Interaktiv menyu:
    0 — Node-0 (LLM + TTS UZ)
    1 — Node-1 (STT RU + TTS RU/EN)
    2 — Node-2 (STT EN + STT UZ)
    a — Barcha node'lar
    q — Chiqish
"""

import argparse
import os
import queue
import subprocess
import sys
import threading
import time

# ============================================================
# ANSI ranglar
# ============================================================
R = "\033[0m"
B = "\033[1m"
GRN = "\033[32m"
CYN = "\033[36m"
MAG = "\033[35m"
YEL = "\033[33m"
DIM = "\033[2m"
BYEL = "\033[93m"

# Node ranglari
NODE_COLORS = {
    0: f"{GRN}{B}",   # Node-0: GREEN
    1: f"{CYN}{B}",   # Node-1: CYAN
    2: f"{MAG}{B}",   # Node-2: MAGENTA
}

NODE_LABELS = {
    0: "Node-0 (LLM+TTS UZ)",
    1: "Node-1 (STT RU+TTS)",
    2: "Node-2 (STT EN+UZ)",
}

NODE_KERNELS = {
    0: "bunyodbek7/ai-operator-kaggle-node",
    1: "bunyodozodboyev/ai-operator-kaggle-node-1",
    2: "bunyodbekozodboyev/ai-operator-kaggle-node-2",
}


def _load_env():
    """Proyekt ildizidagi .env faylini o'qiydi."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    env_path = os.path.join(os.path.dirname(script_dir), ".env")
    if not os.path.exists(env_path):
        print(f"{YEL}⚠️  .env topilmadi: {env_path}{R}")
        return
    with open(env_path, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, val = line.split("=", 1)
            key = key.strip()
            val = val.strip().strip('"').strip("'")
            os.environ[key] = val
            if key == "KAGGLE_KEY" and val.startswith("KGAT_"):
                os.environ["KAGGLE_API_TOKEN"] = val


def _get_kaggle_auth(node_idx: int):
    """Node'ga mos Kaggle autentifikatsiyasini qaytaradi (env vars orqali)."""
    if node_idx == 0:
        user = os.environ.get("KAGGLE_USERNAME", "bunyodbek7")
        key = os.environ.get("KAGGLE_KEY", "")
    elif node_idx == 1:
        user = os.environ.get("KAGGLE_USERNAME_1", "")
        key = os.environ.get("KAGGLE_KEY_1", "")
    else:
        user = os.environ.get("KAGGLE_USERNAME_2", "")
        key = os.environ.get("KAGGLE_KEY_2", "")

    return user, key


def _stream_node_logs(node_idx: int, log_queue: queue.Queue, stop_event: threading.Event):
    """Bitta node uchun kaggle kernels logs -f ni stream qiladi."""
    kernel_id = NODE_KERNELS[node_idx]
    user, key = _get_kaggle_auth(node_idx)

    if not key:
        log_queue.put((node_idx, "system", f"⚠️  Kaggle kaliti topilmadi (NODE{node_idx}_KEY)"))
        return

    env = os.environ.copy()
    env["KAGGLE_USERNAME"] = user
    env["KAGGLE_KEY"] = key

    color = NODE_COLORS[node_idx]
    label = f"[Node-{node_idx}]"

    log_queue.put((node_idx, "system", f"🔌 {NODE_LABELS[node_idx]} ga ulanilmoqda..."))
    log_queue.put((node_idx, "system", f"   Kernel: {kernel_id}, User: {user}"))

    try:
        proc = subprocess.Popen(
            ["kaggle", "kernels", "logs", kernel_id, "-f"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env=env,
            bufsize=1,
        )

        for line in iter(proc.stdout.readline, ""):
            if stop_event.is_set():
                proc.terminate()
                break
            line = line.rstrip("\n")
            if line:
                log_queue.put((node_idx, "log", line))

        proc.wait(timeout=5)
        if proc.returncode != 0 and not stop_event.is_set():
            log_queue.put((node_idx, "system", f"⚠️  Stream uzildi (exit={proc.returncode})"))

    except FileNotFoundError:
        log_queue.put((node_idx, "error", "❌ 'kaggle' CLI topilmadi. pip install kaggle"))
    except Exception as e:
        log_queue.put((node_idx, "error", f"❌ Xatolik: {e}"))


def _display_logs(log_queue: queue.Queue, stop_event: threading.Event, node_count: int):
    """Log queue'dan o'qib, rangli chiqaradi."""
    while not stop_event.is_set():
        try:
            node_idx, msg_type, message = log_queue.get(timeout=0.1)
        except queue.Empty:
            continue

        color = NODE_COLORS.get(node_idx, R)
        ts = time.strftime("%H:%M:%S")

        if msg_type == "system":
            print(f"{color}[Node-{node_idx}]{R} {DIM}[{ts}]{R} {BYEL}{message}{R}")
        elif msg_type == "error":
            print(f"{color}[Node-{node_idx}]{R} {DIM}[{ts}]{R} \033[31m{message}{R}")
        else:
            # Log qatorida [Node-X] bo'lsa, qo'shma — bo'lmasa qo'shamiz
            if f"[Node-{node_idx}]" not in message:
                print(f"{color}[Node-{node_idx}]{R} {DIM}[{ts}]{R} {message}")
            else:
                print(f"{color}{message}{R}")


def _show_menu():
    """Interaktiv menyu."""
    print(f"\n{B}╔══════════════════════════════════╗{R}")
    print(f"{B}║{R}   {BYEL}Kaggle Node Log Streamer{R}     {B}║{R}")
    print(f"{B}╠══════════════════════════════════╣{R}")
    print(f"{B}║{R}  {GRN}0{R} — {NODE_LABELS[0]:22s} {B}║{R}")
    print(f"{B}║{R}  {CYN}1{R} — {NODE_LABELS[1]:22s} {B}║{R}")
    print(f"{B}║{R}  {MAG}2{R} — {NODE_LABELS[2]:22s} {B}║{R}")
    print(f"{B}║{R}  {BYEL}a{R} — Barcha node'lar                {B}║{R}")
    print(f"{B}║{R}  {DIM}q{R} — Chiqish                        {B}║{R}")
    print(f"{B}╚══════════════════════════════════╝{R}")
    print()


def main():
    parser = argparse.ArgumentParser(
        description="Kaggle Node Log Streamer — real-time log streaming",
    )
    parser.add_argument("--node", type=str, default=None,
                        help="Node raqami (0,1,2, all, yoki 0,1,2)")
    args = parser.parse_args()

    _load_env()

    # Node'ni aniqlash
    if args.node is not None:
        node_arg = args.node.strip().lower()
        if node_arg == "all":
            selected = [0, 1, 2]
        elif "," in node_arg:
            selected = []
            for part in node_arg.split(","):
                try:
                    n = int(part.strip())
                    if n in (0, 1, 2):
                        selected.append(n)
                except ValueError:
                    pass
            if not selected:
                print("Noto'g'ri node raqami. 0, 1, 2, all")
                sys.exit(1)
        else:
            try:
                n = int(node_arg)
                if n not in (0, 1, 2):
                    raise ValueError
                selected = [n]
            except ValueError:
                print("Noto'g'ri node raqami. 0, 1, 2, all")
                sys.exit(1)
    else:
        # Interaktiv menyu
        _show_menu()
        choice = input(f"{BYEL}Node tanlang (0/1/2/a/q):{R} ").strip().lower()

        if choice == "q":
            print("Chiqildi.")
            return
        elif choice == "a":
            selected = [0, 1, 2]
        elif choice in ("0", "1", "2"):
            selected = [int(choice)]
        else:
            print("Noto'g'ri tanlov.")
            return

    if not selected:
        print("Hech qanday node tanlanmadi.")
        return

    print(f"\n{B}Stream boshlanmoqda: {', '.join(str(n) for n in selected)}{R}")
    print(f"{DIM}(Ctrl+C bilan to'xtating){R}\n")

    log_queue = queue.Queue()
    stop_event = threading.Event()

    # Har bir node uchun alohida stream thread
    threads = []
    for n in selected:
        t = threading.Thread(target=_stream_node_logs, args=(n, log_queue, stop_event), daemon=True)
        t.start()
        threads.append(t)

    try:
        _display_logs(log_queue, stop_event, len(selected))
    except KeyboardInterrupt:
        print(f"\n{YEL}To'xtatilmoqda...{R}")
    finally:
        stop_event.set()
        for t in threads:
            t.join(timeout=3)
        print(f"{DIM}Log streamer to'xtadi.{R}")


if __name__ == "__main__":
    main()
