#!/usr/bin/env python3
"""
Kaggle Node Log Streamer — Real-time log streaming.

Har bir node log'ini alohida rangda va [Node-X] prefiks bilan ko'rsatadi.

**Yangi: Ko'p node rejimida panel ko'rinish** — log'lar aralashib ketmaydi,
har bir node ekranning o'z qismida mustaqil yangilanadi.

Ishlatish:
    python3 scripts/log_stream.py              # interaktiv menyu
    python3 scripts/log_stream.py --node 0     # faqat Node-0 (stream)
    python3 scripts/log_stream.py --node all   # barcha node'lar (panel)
    python3 scripts/log_stream.py --node 0,2   # Node-0 va Node-2 (panel)
    python3 scripts/log_stream.py --node all --lines 15  # har panelda 15 qator

Interaktiv menyu:
    0 — Node-0 (LLM + TTS UZ)
    1 — Node-1 (STT RU + TTS RU/EN)
    2 — Node-2 (STT EN + STT UZ)
    a — Barcha node'lar
    q — Chiqish
"""

import argparse
import os
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
RED = "\033[31m"

# Node ranglari
NODE_COLORS = {
    0: f"{GRN}{B}",
    1: f"{CYN}{B}",
    2: f"{MAG}{B}",
}

NODE_BG = {
    0: GRN,
    1: CYN,
    2: MAG,
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

LOG_DIR = "/tmp/kaggle_logs"
STATUS_DIR = "/tmp/kaggle_logs/.status"


def _load_env():
    """Proyekt ildizidagi .env faylini os.environ'ga yuklaydi."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    env_path = os.path.join(os.path.dirname(script_dir), ".env")
    if not os.path.exists(env_path):
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
            if key.startswith("KAGGLE_KEY") and val.startswith("KGAT_"):
                if key == "KAGGLE_KEY":
                    os.environ["KAGGLE_API_TOKEN"] = val
                else:
                    os.environ[f"KAGGLE_API_TOKEN_{key[-1]}"] = val


def _get_kaggle_auth(node_idx: int):
    """Node'ga mos Kaggle autentifikatsiyasini qaytaradi."""
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


def _make_env(node_idx: int):
    """Node uchun toza subprocess environment yaratadi."""
    user, key = _get_kaggle_auth(node_idx)
    env = os.environ.copy()
    env["KAGGLE_USERNAME"] = user
    env["KAGGLE_KEY"] = key
    # KAGGLE_API_TOKEN ni tozalab, joriy node'ga mosini o'rnatamiz
    env.pop("KAGGLE_API_TOKEN", None)
    api_token_key = "KAGGLE_API_TOKEN" if node_idx == 0 else f"KAGGLE_API_TOKEN_{node_idx}"
    api_token = os.environ.get(api_token_key, "")
    if api_token:
        env["KAGGLE_API_TOKEN"] = api_token
    return env


def _stream_to_file(node_idx: int, stop_event: threading.Event):
    """kaggle kernels logs -f dan o'qib, faylga yozadi (handle ochiq)."""
    kernel_id = NODE_KERNELS[node_idx]
    log_file = os.path.join(LOG_DIR, f"node_{node_idx}.log")
    status_file = os.path.join(STATUS_DIR, f"node_{node_idx}.status")
    env = _make_env(node_idx)
    user, _ = _get_kaggle_auth(node_idx)

    def _write_status(status: str):
        try:
            with open(status_file, "w") as sf:
                sf.write(status)
        except Exception:
            pass

    _write_status("connecting")

    # Faylni ochamiz va loop davomida ochiq saqlaymiz
    lf = open(log_file, "a", buffering=1)  # line-buffered
    try:
        lf.write(f"── 🔌 {NODE_LABELS[node_idx]} ga ulanilmoqda (kernel: {kernel_id}, user: {user})\n")
        lf.flush()

        proc = subprocess.Popen(
            ["kaggle", "kernels", "logs", kernel_id, "-f"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env=env,
            bufsize=1,
        )
        _write_status("streaming")

        for line in iter(proc.stdout.readline, ""):
            if stop_event.is_set():
                proc.terminate()
                break
            line = line.rstrip("\n")
            if line:
                lf.write(line + "\n")
                lf.flush()

        proc.wait(timeout=5)
        if proc.returncode != 0 and not stop_event.is_set():
            lf.write(f"── ⚠️  Stream uzildi (exit={proc.returncode})\n")
            lf.flush()
            _write_status(f"dead:{proc.returncode}")
        else:
            _write_status("ended")

    except FileNotFoundError:
        lf.write("── ❌ 'kaggle' CLI topilmadi\n")
        lf.flush()
        _write_status("error:no_cli")
    except Exception as e:
        lf.write(f"── ❌ Xatolik: {e}\n")
        lf.flush()
        _write_status(f"error:{e}")
    finally:
        lf.close()


def _stream_to_stdout(node_idx: int, stop_event: threading.Event):
    """Bitta node uchun to'g'ridan-to'g'ri stdout ga stream."""
    kernel_id = NODE_KERNELS[node_idx]
    env = _make_env(node_idx)
    user, _ = _get_kaggle_auth(node_idx)
    color = NODE_COLORS[node_idx]

    print(f"\n{color}[Node-{node_idx}]{R} {DIM}[{time.strftime('%H:%M:%S')}]{R} {BYEL}🔌 {NODE_LABELS[node_idx]} ga ulanilmoqda...{R}")
    print(f"{color}[Node-{node_idx}]{R} {DIM}[{time.strftime('%H:%M:%S')}]{R}    Kernel: {kernel_id}, User: {user}")

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
                ts = time.strftime("%H:%M:%S")
                print(f"{color}[Node-{node_idx}]{R} {DIM}[{ts}]{R} {line}")

        proc.wait(timeout=5)
        if proc.returncode != 0 and not stop_event.is_set():
            ts = time.strftime("%H:%M:%S")
            print(f"{color}[Node-{node_idx}]{R} {DIM}[{ts}]{R} {RED}⚠️  Stream uzildi (exit={proc.returncode}){R}")

    except FileNotFoundError:
        print(f"{RED}❌ 'kaggle' CLI topilmadi. pip install kaggle{R}")
    except Exception as e:
        print(f"{RED}❌ Xatolik: {e}{R}")


def _read_last_lines(filepath: str, n: int, positions: dict) -> list:
    """Fayldan oxirgi n ta qatorni o'qiydi. positions orqali yangi qatorlarni kuzatadi."""
    try:
        fsize = os.path.getsize(filepath)
        last_pos = positions.get(filepath, 0)
        if fsize <= last_pos:
            # Fayl o'zgarmagan — keshdan qaytaramiz
            return positions.get(f"{filepath}:cache", [])

        with open(filepath, "r") as f:
            f.seek(max(0, fsize - 16384))  # oxirgi ~16KB ni o'qiymiz
            if f.tell() > 0:
                f.readline()  # birinchi yarim qatorni o'tkazib yuboramiz
            lines = [l.rstrip("\n") for l in f.readlines()]

        positions[filepath] = fsize
        result = lines[-n:]
        positions[f"{filepath}:cache"] = result
        return result
    except FileNotFoundError:
        return []


def _read_status(node_idx: int) -> str:
    """Node stream statusini o'qiydi."""
    try:
        with open(os.path.join(STATUS_DIR, f"node_{node_idx}.status"), "r") as sf:
            return sf.read().strip()
    except FileNotFoundError:
        return "starting"


def _count_lines(filepath: str) -> int:
    """Fayldagi qatorlar soni."""
    try:
        with open(filepath, "r") as f:
            return sum(1 for _ in f)
    except FileNotFoundError:
        return 0


def _display_panels(nodes: list, lines_per_panel: int, stop_event: threading.Event):
    """Ko'p node rejimi: ekranni tozalab, har bir node'ni alohida panelda ko'rsatadi."""
    positions = {}  # fayl → oxirgi o'qilgan pozitsiya
    sep = f"{DIM}{'─' * 70}{R}"

    while not stop_event.is_set():
        sys.stdout.write("\033[2J\033[H")
        sys.stdout.flush()

        # Header
        total_lines = sum(_count_lines(os.path.join(LOG_DIR, f"node_{n}.log")) for n in nodes)
        print(f"{B}╔══════════════════════════════════════════════════════════════════╗{R}")
        print(f"{B}║{R}  {BYEL}Kaggle Log Streamer — Panel View{R}        {DIM}[Ctrl+C = chiqish]{R}      {B}║{R}")
        print(f"{B}║{R}  {DIM}Jami log qatorlari: {total_lines}{R}                                       {B}║{R}")
        print(f"{B}╚══════════════════════════════════════════════════════════════════╝{R}")
        print()

        for n in nodes:
            color = NODE_COLORS[n]
            bg = NODE_BG[n]
            log_file = os.path.join(LOG_DIR, f"node_{n}.log")
            status = _read_status(n)

            # Status badge
            if status == "streaming":
                badge = f"{GRN}● LIVE{R}"
            elif status.startswith("dead"):
                badge = f"{RED}✗ DEAD (exit={status.split(':')[1] if ':' in status else '?'}){R}"
            elif status.startswith("error"):
                badge = f"{RED}✗ ERROR{R}"
            elif status == "ended":
                badge = f"{YEL}◉ ENDED{R}"
            elif status == "connecting":
                badge = f"{YEL}◌ CONNECTING...{R}"
            else:
                badge = f"{DIM}...{R}"

            line_count = _count_lines(log_file)
            last_lines = _read_last_lines(log_file, lines_per_panel, positions)

            # Panel header
            title = f"Node-{n}: {NODE_LABELS[n]}"
            info = f"{line_count} qator"
            pad = 53 - len(title) - len(info)
            pad = max(1, pad)
            print(f"{color}┌─ {title}  {DIM}{info}{R} {badge} {'─' * pad}┐{R}")

            if not last_lines:
                if status == "connecting":
                    print(f"{color}│{R} {DIM}(ulanilmoqda...){R}")
                else:
                    print(f"{color}│{R} {DIM}(hali log yo'q — kuting...){R}")
            else:
                for line in last_lines:
                    display_line = line[:120]
                    print(f"{color}│{R} {display_line}")

            print(f"{color}└{'─' * 68}┘{R}")
            print()

        print(sep)
        print(f"{DIM}Yangilanish: {time.strftime('%H:%M:%S')}  |  Ctrl+C = chiqish{R}")

        time.sleep(1)


def _show_menu():
    """Interaktiv menyu."""
    print(f"\n{B}╔══════════════════════════════════╗{R}")
    print(f"{B}║{R}   {BYEL}Kaggle Node Log Streamer{R}     {B}║{R}")
    print(f"{B}╠══════════════════════════════════╣{R}")
    print(f"{B}║{R}  {GRN}0{R} — {NODE_LABELS[0]:22s} {B}║{R}")
    print(f"{B}║{R}  {CYN}1{R} — {NODE_LABELS[1]:22s} {B}║{R}")
    print(f"{B}║{R}  {MAG}2{R} — {NODE_LABELS[2]:22s} {B}║{R}")
    print(f"{B}║{R}  {BYEL}a{R} — Barcha node'lar (panel)       {B}║{R}")
    print(f"{B}║{R}  {DIM}q{R} — Chiqish                        {B}║{R}")
    print(f"{B}╚══════════════════════════════════╝{R}")
    print()


def main():
    parser = argparse.ArgumentParser(
        description="Kaggle Node Log Streamer — real-time log streaming",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Misollar:\n"
            "  python3 scripts/log_stream.py              # interaktiv menyu\n"
            "  python3 scripts/log_stream.py --node 0     # faqat Node-0 (stream)\n"
            "  python3 scripts/log_stream.py --node all   # barcha node (panel)\n"
            "  python3 scripts/log_stream.py --node 0,2   # Node-0 + 2 (panel)\n"
            "  python3 scripts/log_stream.py --node all --lines 15  # 15 qator/panel\n"
        ),
    )
    parser.add_argument("--node", type=str, default=None,
                        help="Node raqami (0,1,2, all, yoki 0,1,2)")
    parser.add_argument("--lines", type=int, default=10,
                        help="Panel rejimda har node uchun ko'rsatiladigan qatorlar soni (default: 10)")
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

    stop_event = threading.Event()
    is_single = len(selected) == 1

    if is_single:
        # 🔹 Bitta node — to'g'ridan-to'g'ri stream
        n = selected[0]
        print(f"\n{B}Stream: {NODE_LABELS[n]}{R}")
        print(f"{DIM}(Ctrl+C bilan to'xtating){R}\n")

        t = threading.Thread(target=_stream_to_stdout, args=(n, stop_event), daemon=True)
        t.start()

        try:
            while t.is_alive():
                t.join(1)
        except KeyboardInterrupt:
            pass
    else:
        # 🔹 Ko'p node — panel rejimi (faylga yozish + ekranni yangilash)
        os.makedirs(LOG_DIR, exist_ok=True)
        os.makedirs(STATUS_DIR, exist_ok=True)

        # Eski log fayllarni tozalash
        for n in selected:
            log_file = os.path.join(LOG_DIR, f"node_{n}.log")
            open(log_file, "w").close()

        print(f"\n{B}Panel rejimi: {', '.join(NODE_LABELS[n] for n in selected)}{R}")
        print(f"{DIM}Har bir node {args.lines} ta qator, har 1s yangilanadi{R}")
        print(f"{DIM}(Ctrl+C bilan to'xtating){R}\n")
        time.sleep(1)

        # Stream thread'lar
        threads = []
        for n in selected:
            t = threading.Thread(target=_stream_to_file, args=(n, stop_event), daemon=True)
            t.start()
            threads.append(t)

        # Panel display (asosiy thread)
        try:
            _display_panels(selected, args.lines, stop_event)
        except KeyboardInterrupt:
            pass

    # Cleanup
    print(f"\n{YEL}To'xtatilmoqda...{R}")
    stop_event.set()
    time.sleep(0.5)

    if not is_single:
        for n in selected:
            log_file = os.path.join(LOG_DIR, f"node_{n}.log")
            total = _count_lines(log_file)
            print(f"{NODE_COLORS[n]}Node-{n}:{R} {total} qator yozildi → {log_file}")

    print(f"{DIM}Log streamer to'xtadi.{R}")


if __name__ == "__main__":
    main()
