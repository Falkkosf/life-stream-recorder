#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Multi-Platform Live Stream Recorder
Поддержка: Twitch, YouTube, Kick, Rumble, Trovo, W.tv, Wasd.tv + любой yt-dlp источник

АРХИТЕКТУРА (без разрывов, без WinError 32):
  streamlink --stdout (Twitch) или yt-dlp -o - (остальные)
      |
      v  pipe
  ffmpeg -f segment -segment_time N -c copy seg_%04d.ts

  ffmpeg segment muxer сам режет поток по GOP-границам.
  Каждый закрытый файл сразу готов к монтажу — никаких блокировок.
  Поток загрузки не прерывается ни на миллисекунду.

  SegmentWatcher следит за папкой и логирует каждый готовый файл.

ИСПРАВЛЕНИЯ В ЭТОЙ ВЕРСИИ:
  1) Кнопка "Стоп" не убивала процесс до конца:
     yt-dlp/streamlink сами порождают дочерний ffmpeg, а proc.terminate()
     в Windows убивает только один PID, не трогая потомков. Теперь используется
     taskkill /F /T (Windows) или killpg (POSIX) — убивается всё дерево процессов.
     Дополнительно все дочерние процессы запускаются в новой группе процессов
     (CREATE_NEW_PROCESS_GROUP), чтобы taskkill /T гарантированно их находил.

  2) Старый режим докачки растущего VOD удалён из рабочего пути:
     a) Изначально резали VOD на узкие тайм-окна через
        yt-dlp --download-sections "*START-END" в цикле. У ещё растущего VOD
        плейлист index-dvr.m3u8 скользящий — к моменту скачивания окно "уезжает"
        (лог: "The m3u8 list sequence may have been wrapped"), yt-dlp/ffmpeg
        зависали, ничего не скачивая (size=0KiB).
     b) Попытка заменить это ОДНИМ непрерывным запуском с открытым диапазоном
        "*START-" (без конца) через pipe → ffmpeg -f segment тоже не сработала:
        экстрактор twitch:vod у yt-dlp не умеет "хвостить" растущий live-плейлист
        бесконечно через pipe — он один раз скачивает то, что доступно в плейлисте
        В МОМЕНТ ЗАПРОСА, и завершается кодом 0.
     c) Третий баг (найден по логам с реальными таймингами): цикл "ждать
        segment_sec (например 30 минут) → скачать с открытым диапазоном от
        зафиксированного заранее offset" тоже ломался с тем же "wrapped" /
        size=0KiB, потому что DVR-буфер Twitch для ЕЩЁ ИДУЩЕЙ трансляции —
        это не вся история с начала, а короткий скользящий буфер (по логам —
        порядка 60-90 секунд). offset, посчитанный ДО ожидания, к моменту
        реального запроса уже успевал выпасть из этого окна, если ждали
        дольше, чем сам буфер.
     ИТОГОВОЕ РЕШЕНИЕ: разделили два независимых понятия —
       - как часто нужно "трогать" поток, чтобы не вывалиться из короткого
         DVR-буфера (VOD_POLL_INTERVAL, ~20 сек — заведомо меньше буфера);
       - какого размера должен быть итоговый файл на диске (segment_minutes
         из настроек, как и раньше).
     Теперь мы часто (каждые VOD_POLL_INTERVAL) докачиваем маленькие куски
     открытым диапазоном "*START-" в отдельную скрытую временную папку .vod_tmp, измеряем
     фактическую длительность каждого куска через ffprobe (не полагаясь на
     предположения) и по мере накопления сдвигаем стартовую точку.
     Как только суммарная длительность накопленных кусков достигает
     configured segment_sec — склеиваем их в один итоговый файл через
     ffmpeg -f concat -c copy (без перекодирования — все куски из одного
     и того же источника с одинаковыми параметрами кодека) и начинаем
     новый батч. Финальный "хвост" (то, что не дотянуло до порога) тоже
     склеивается при остановке/завершении стрима — чтобы ничего не терялось.

  3) Добавлен выбор режима записи на каждой карточке — "🔴 Поток" или "📼 VOD".
     В режиме VOD по нику Twitch программа ждёт только ONLINE и сразу пишет
     live-поток в сегменты. Это позволяет монтировать первые файлы, пока эфир
     ещё идёт:
       - через GQL берём список последних архивных VOD канала и статус
         "онлайн/оффлайн";
       - смотрим, растёт ли длительность найденного VOD (сравниваем через
         yt-dlp дважды с паузой VOD_POLL_INTERVAL) — если да, значит эфир
         идёт прямо сейчас и архив ещё дописывается;
       - если НЕ растёт (эфир уже закончился/это старый VOD) — просто качаем
         его целиком обычным конвейером (streamlink/yt-dlp → ffmpeg segment),
         с нормальной нарезкой по segment_minutes;
       - если растёт — сначала ОДНИМ файлом, без сегментации, докачиваем
         то, что уже гарантированно устоялось и не изменится (см.
         _download_stable_prefix), и только ПОСЛЕ ЭТОГО включаем поллинг
         "только новое" (VOD_POLL_INTERVAL) для хвоста, который ещё
         дописывается — с той же склейкой кусков в файлы по segment_minutes,
         что и раньше.
     Также убрана обрезка "на стыке" (CHUNK_TRIM_SECONDS/_trim_chunk_head):
     она была лишним костылём поверх обрезки и норовила зависать на -ss по
     большим .ts/.mp4 файлам. В ней не было смысла — каждый следующий кусок
     и так качается строго от offset, посчитанного по фактически измеренной
     ffprobe длительности предыдущего куска, так что дублей на стыке не
     возникает без всякой пост-обработки.

  4) БАГ ПОВТОРНОГО СКАЧИВАНИЯ ОДНОГО И ТОГО ЖЕ VOD (найден по логам:
     Стримы #2-#5 качали один и тот же VOD id=2859239650 заново каждые
     30 секунд, хотя стрим уже закончился и новых данных не было):
     В run() условие выхода из цикла "ждать следующий стрим" проверяло
     только extract_twitch_vod_id(self.channel) — то есть случай, когда
     пользователь вручную вставил прямую ссылку на конкретный VOD. При
     автопоиске по нику (mode="vod", self.channel — это просто ник канала)
     это условие всегда было False, поэтому после завершения записи код
     считал, что нужно "ждать следующий стрим" — а на деле
     _resolve_twitch_vod_auto() снова находил тот же самый последний
     архивный VOD (канал офлайн, новых архивов нет) и запись начиналась
     заново с нуля, до бесконечности, пока пользователь не жал "Стоп".
     Исправление: recorder запоминает id последнего полностью обработанного
     VOD (self._last_finished_vod_id). Если при следующем автопоиске найден
     VOD с тем же id и канал по-прежнему офлайн — считаем, что новых данных
     нет, и продолжаем ждать (без повторного скачивания), вместо того чтобы
     сразу качать его заново. Если стример запустит новый эфир — появится
     новый vod_id, и запись начнётся как обычно.

Требует:
  pip install customtkinter yt-dlp streamlink
  ffmpeg в PATH
"""

import tkinter as tk
from tkinter import filedialog, messagebox
import customtkinter as ctk
import threading
import subprocess
import json
import re
import glob as glob_mod
import queue
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys
import urllib.request
import time
from datetime import datetime, timezone
from pathlib import Path
from enum import Enum

# ─── Конфигурация ─────────────────────────────────────────────────────────────
CHECK_INTERVAL = 5
CONFIG_PATH    = Path(__file__).parent / "live_recorder_config.json"

# Как часто "трогаем" ещё идущий VOD в Debug-режиме докачки только нового.
# ДОЛЖНО быть заведомо меньше короткого скользящего DVR-буфера Twitch для
# растущей трансляции (по наблюдениям в логах — порядка 60-90 сек), иначе
# offset успевает "уехать" из окна и yt-dlp зависает на size=0KiB с
# "The m3u8 list sequence may have been wrapped". Это НЕ размер итогового
# файла — тот по-прежнему задаётся полем "Сегмент (мин)" в UI и достигается
# склейкой накопленных кусков (см. _concat_chunks).
VOD_POLL_INTERVAL = 10

TWITCH_GQL  = "https://gql.twitch.tv/gql"
GQL_HEADERS = {
    "Client-ID": "kimne78kx3ncx6brgo4mv6wki5h1ko",
    "Content-Type": "application/json",
}

# ─── Платформы ────────────────────────────────────────────────────────────────
class Platform(Enum):
    TWITCH  = "Twitch"
    YOUTUBE = "YouTube"
    KICK    = "Kick"
    RUMBLE  = "Rumble"
    TROVO   = "Trovo"
    WTV     = "W.tv"
    WASD    = "Wasd.tv"
    CUSTOM  = "Custom URL"

PLATFORM_ICONS = {
    Platform.TWITCH:  "🟣",
    Platform.YOUTUBE: "🔴",
    Platform.KICK:    "🟢",
    Platform.RUMBLE:  "🟠",
    Platform.TROVO:   "🔵",
    Platform.WTV:     "⚪",
    Platform.WASD:    "🟡",
    Platform.CUSTOM:  "🌐",
}

PLATFORM_URL_TEMPLATES = {
    Platform.TWITCH:  "https://www.twitch.tv/{channel}",
    Platform.YOUTUBE: "https://www.youtube.com/@{channel}/live",
    Platform.KICK:    "https://kick.com/{channel}",
    Platform.RUMBLE:  "https://rumble.com/c/{channel}/live",
    Platform.TROVO:   "https://trovo.live/{channel}",
    Platform.WTV:     "https://w.tv/{channel}",
    Platform.WASD:    "https://wasd.tv/{channel}",
    Platform.CUSTOM:  "{channel}",
}

# ─── Тема ─────────────────────────────────────────────────────────────────────
ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("dark-blue")

BG_DEEP   = "#090910"
BG_PANEL  = "#0f0d1c"
BG_CARD   = "#14102b"
BG_HOVER  = "#1c1640"
PURPLE    = "#7c3aed"
PURPLE_LT = "#a855f7"
PURPLE_DK = "#4c1d95"
WHITE     = "#f0eeff"
GRAY      = "#6b7280"
GRAY_LT   = "#9ca3af"
GREEN     = "#10b981"
RED       = "#ef4444"
YELLOW    = "#f59e0b"
BORDER    = "#2d1f5e"
TEAL      = "#14b8a6"

PLATFORM_COLORS = {
    Platform.TWITCH:  "#9147ff",
    Platform.YOUTUBE: "#ff0000",
    Platform.KICK:    "#53fc18",
    Platform.RUMBLE:  "#85c742",
    Platform.TROVO:   "#19d66b",
    Platform.WTV:     "#ffffff",
    Platform.WASD:    "#fbbf24",
    Platform.CUSTOM:  "#60a5fa",
}

# ─── Утилиты ──────────────────────────────────────────────────────────────────

def _subprocess_flags() -> int:
    """
    Флаги для запуска дочерних процессов на Windows:
      CREATE_NO_WINDOW          — не открывать консольное окно
      CREATE_NEW_PROCESS_GROUP  — своя группа процессов, нужна для того,
                                   чтобы taskkill /T находил и убивал ВСЕ
                                   дочерние процессы (например ffmpeg,
                                   которого сам порождает yt-dlp внутри себя)
    """
    if sys.platform == "win32":
        return subprocess.CREATE_NO_WINDOW | subprocess.CREATE_NEW_PROCESS_GROUP
    return 0


def kill_process_tree(pid: int, timeout: float = 8.0):
    """
    Убивает процесс и ВСЕ его дочерние процессы.

    Проблема, которую это чинит: yt-dlp (и streamlink при определённых
    сценариях) сам порождает дочерний ffmpeg-процесс для мультиплексирования
    HLS-сегментов. Обычный Popen.terminate() на Windows делает
    TerminateProcess() только над указанным PID и НЕ трогает потомков —
    в результате yt-dlp завершался, а его дочерний ffmpeg продолжал
    висеть и качать в фоне даже после нажатия "Стоп".

    Решение:
      Windows — taskkill /F /T /PID <pid>  (флаг /T = убить дерево процессов)
      POSIX   — killpg по группе процессов (запускать через start_new_session)
    """
    if sys.platform == "win32":
        try:
            subprocess.run(
                ["taskkill", "/F", "/T", "/PID", str(pid)],
                capture_output=True,
                creationflags=subprocess.CREATE_NO_WINDOW,
                timeout=timeout,
            )
        except Exception:
            pass
    else:
        try:
            import os, signal
            os.killpg(os.getpgid(pid), signal.SIGKILL)
        except Exception:
            try:
                import os, signal
                os.kill(pid, signal.SIGKILL)
            except Exception:
                pass


def safe_filename(s: str) -> str:
    s = re.sub(r'[\\/:*?"<>|]', "_", s)
    s = re.sub(r'\s+', " ", s).strip()
    return s[:80] or "stream"

def sec_to_hms(sec: int) -> str:
    sec = max(0, int(sec))
    h, r = divmod(sec, 3600)
    m, s = divmod(r, 60)
    return f"{h}:{m:02d}:{s:02d}"

def gql_request(query: str, variables: dict = None):
    payload = {"query": query}
    if variables:
        payload["variables"] = variables
    data = json.dumps(payload).encode()
    req = urllib.request.Request(TWITCH_GQL, data=data, headers=GQL_HEADERS)
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=15) as r:
                return json.loads(r.read())
        except OSError:
            if attempt < 2:
                time.sleep(2)
            else:
                raise

def fetch_stream_info_twitch(login: str):
    query = """
    query($login: String!) {
      user(login: $login) {
        id login displayName
        stream { id title game { name } createdAt viewersCount }
      }
    }"""
    try:
        result = gql_request(query, {"login": login})
        user = result.get("data", {}).get("user")
        if not user:
            return None, None
        stream = user.get("stream")
        if not stream:
            return user, None
        started_at = datetime.strptime(
            stream["createdAt"], "%Y-%m-%dT%H:%M:%SZ"
        ).replace(tzinfo=timezone.utc)
        return user, {
            "stream_id":  stream["id"],
            "title":      stream.get("title") or user.get("displayName", login),
            "game":       (stream.get("game") or {}).get("name", ""),
            "started_at": started_at,
        }
    except Exception:
        return None, None

def check_stream_live_ytdlp(url: str) -> tuple:
    try:
        flags = _subprocess_flags()
        result = subprocess.run(
            ["yt-dlp", "--no-download", "--print", "%(is_live)s|||%(title)s",
             "--socket-timeout", "15", url],
            capture_output=True, text=True, timeout=30, creationflags=flags
        )
        out = result.stdout.strip()
        if "|||" in out:
            parts = out.split("|||", 1)
            is_live = parts[0].strip().lower() in ("true", "1", "yes")
            return is_live, (parts[1].strip() if len(parts) > 1 else "stream")
        return False, "stream"
    except Exception:
        return False, "stream"

def fetch_vod_title(url: str) -> str:
    """Достаёт название VOD/видео через yt-dlp, не скачивая сам файл."""
    try:
        flags = _subprocess_flags()
        result = subprocess.run(
            ["yt-dlp", "--no-download", "--print", "%(title)s",
             "--socket-timeout", "15", url],
            capture_output=True, text=True, timeout=30, creationflags=flags
        )
        out = result.stdout.strip().splitlines()
        return out[0].strip() if out and out[0].strip() else "VOD"
    except Exception:
        return "VOD"

TWITCH_VOD_RE     = re.compile(r'^https?://(?:www\.)?twitch\.tv/videos/(\d+)', re.IGNORECASE)
TWITCH_CHANNEL_RE = re.compile(r'^https?://(?:www\.)?twitch\.tv/([a-zA-Z0-9_]+)/?$', re.IGNORECASE)
_TWITCH_RESERVED  = {"videos", "directory", "p", "settings", "subs", "downloads", "friends"}

def extract_twitch_vod_id(text: str):
    """Если введена прямая ссылка на VOD Twitch — возвращает его id, иначе None."""
    m = TWITCH_VOD_RE.match(text.strip())
    return m.group(1) if m else None

def extract_twitch_channel_login(text: str):
    """Если введена ссылка вида twitch.tv/<канал> (не VOD) — возвращает логин, иначе None."""
    m = TWITCH_CHANNEL_RE.match(text.strip())
    if not m:
        return None
    login = m.group(1)
    return None if login.lower() in _TWITCH_RESERVED else login

def is_url(text: str) -> bool:
    return text.strip().lower().startswith(("http://", "https://"))

def probe_file_duration_seconds(path: Path):
    """
    Измеряет реальную длительность уже скачанного файла через ffprobe.

    Используется debug-режимом докачки VOD: вместо того чтобы ПРЕДПОЛАГАТЬ,
    сколько секунд должно было накопиться в очередном куске, мы измеряем,
    сколько реально скачалось, и по этому фактическому числу сдвигаем
    стартовую точку для следующей итерации.
    """
    try:
        flags = _subprocess_flags()
        result = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
             "-of", "default=noprint_wrappers=1:nokey=1", str(path)],
            capture_output=True, text=True, timeout=15, creationflags=flags
        )
        val = result.stdout.strip()
        if val and val.replace(".", "", 1).isdigit():
            return float(val)
    except Exception:
        pass
    return None

def fetch_vod_duration_seconds(url: str):
    """
    Текущая доступная длительность VOD (в секундах) через yt-dlp.
    Для ещё растущего VOD (запись идёт прямо сейчас, стример ещё в эфире)
    это фактически «сколько уже есть в VOD на данный момент» —
    используется debug-режимом «качать только новое».
    """
    try:
        flags = _subprocess_flags()
        result = subprocess.run(
            ["yt-dlp", "--no-download", "--print", "%(duration)s",
             "--socket-timeout", "15", url],
            capture_output=True, text=True, timeout=30, creationflags=flags
        )
        out = result.stdout.strip().splitlines()
        if out:
            val = out[0].strip()
            if val and val.replace(".", "", 1).isdigit():
                return float(val)
    except Exception:
        pass
    return None

def fetch_twitch_videos(login: str, first: int = 3):
    """
    Список последних архивных VOD канала (свежие сначала) через тот же
    Twitch GQL, что уже используется для проверки "онлайн/оффлайн".
    Нужно для автопоиска VOD в режиме "VOD" по одному только нику —
    без прямой ссылки на конкретное видео.
    """
    query = """
    query($login: String!, $first: Int!) {
      user(login: $login) {
        videos(first: $first, type: ARCHIVE, sort: TIME) {
          edges {
            node { id title createdAt lengthSeconds }
          }
        }
      }
    }"""
    try:
        result = gql_request(query, {"login": login, "first": first})
        user = result.get("data", {}).get("user")
        if not user:
            return []
        edges = (user.get("videos") or {}).get("edges") or []
        out = []
        for e in edges:
            node = e.get("node") or {}
            if node.get("id"):
                out.append({
                    "id":     node["id"],
                    "title":  node.get("title") or "VOD",
                    "length": node.get("lengthSeconds") or 0,
                })
        return out
    except Exception:
        return []

def bind_paste_fix(entry_widget, var: tk.StringVar = None):
    """
    Чинит вставку в поле ввода.

    Проблема: в Tkinter стандартное сочетание Ctrl+V слушается по keysym,
    а keysym зависит от активной раскладки клавиатуры. При включённой
    русской (или любой нелатинской) раскладке физическая клавиша "V"
    может не давать keysym "v", и штатная вставка перестаёт срабатывать.

    Решение: ловим Control+V дополнительно по коду клавиши (keycode),
    который не зависит от раскладки, и на всякий случай добавляем
    пункт «Вставить» в контекстное меню по ПКМ — оно работает всегда,
    независимо от раскладки и биндингов.
    """
    target = getattr(entry_widget, "_entry", entry_widget)  # для CTkEntry — внутренний tk.Entry

    def _do_paste(event=None):
        try:
            text = target.clipboard_get()
        except Exception:
            return "break"
        text = text.strip()
        try:
            if target.selection_present():
                target.delete("sel.first", "sel.last")
            target.insert("insert", text)
        except Exception:
            try:
                target.delete(0, "end")
                target.insert(0, text)
            except Exception:
                if var is not None:
                    var.set(text)
        return "break"

    def _do_copy():
        try:
            if target.selection_present():
                target.clipboard_clear()
                target.clipboard_append(target.selection_get())
        except Exception:
            pass

    def _do_cut():
        try:
            if target.selection_present():
                target.clipboard_clear()
                target.clipboard_append(target.selection_get())
                target.delete("sel.first", "sel.last")
        except Exception:
            pass

    def _on_ctrl_key(event):
        # 0x4 — Control на X11/Windows; keycode не зависит от раскладки
        if not (event.state & 0x4):
            return
        key = (event.keysym or "").lower()
        code = event.keycode
        if key == "v" or code in (86, 55, 47, 25):
            return _do_paste()
        if key == "c" or code in (67, 54, 8, 38):
            _do_copy()
        elif key == "x" or code in (88, 53):
            _do_cut()
        elif key == "a" or code in (65, 38, 24):
            try:
                target.select_range(0, "end")
            except Exception:
                pass
            return "break"

    target.bind("<Control-KeyPress>", _on_ctrl_key)
    target.bind("<Control-v>", _do_paste)
    target.bind("<Control-V>", _do_paste)

    menu = tk.Menu(target, tearoff=0, bg=BG_CARD, fg=WHITE,
                    activebackground=PURPLE, activeforeground=WHITE, bd=0)
    menu.add_command(label="Вставить", command=_do_paste)
    menu.add_command(label="Копировать", command=_do_copy)
    menu.add_command(label="Вырезать", command=_do_cut)

    def _popup(e):
        try:
            menu.tk_popup(e.x_root, e.y_root)
        finally:
            menu.grab_release()

    target.bind("<Button-3>", _popup)


def load_config() -> dict:
    if CONFIG_PATH.exists():
        try:
            return json.loads(CONFIG_PATH.read_text("utf-8"))
        except Exception:
            pass
    return {
        "sessions":        [],
        "output_path":     str(Path.home() / "Downloads"),
        "segment_minutes": 30,
        "twitch_quality":  "best",
    }

def save_config(cfg: dict):
    try:
        CONFIG_PATH.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), "utf-8")
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════════════════════
#  SegmentWatcher — логирует готовые сегменты из папки
# ═══════════════════════════════════════════════════════════════════════════════

class SegmentWatcher(threading.Thread):
    """
    ffmpeg -f segment создаёт и закрывает файлы сам.
    Мы сканируем папку каждые 3 сек и логируем новые готовые файлы.

    «Готовый» = не является самым свежим по mtime
    (самый свежий — тот который ffmpeg пишет прямо сейчас).
    """

    def __init__(self, work_dir: Path, glob_pattern: str, log_cb, status_cb, stop_event):
        super().__init__(daemon=True)
        self.work_dir     = work_dir
        self.glob_pattern = glob_pattern
        self.log          = log_cb
        self.set_status   = status_cb
        self.stop_event   = stop_event
        self._reported: set = set()
        self._done = threading.Event()

    def join_done(self, timeout: float = 15):
        self._done.wait(timeout)

    def run(self):
        try:
            while not self.stop_event.is_set():
                self._scan(final=False)
                self.stop_event.wait(3)
            time.sleep(0.5)
            self._scan(final=True)
        finally:
            self._done.set()

    def _scan(self, final: bool):
        files = sorted(
            self.work_dir.glob(self.glob_pattern),
            key=lambda f: f.stat().st_mtime if f.exists() else 0
        )
        if not files:
            return
        # Все кроме последнего — гарантированно закрыты ffmpeg
        candidates = files if final else files[:-1]
        for f in candidates:
            if f in self._reported:
                continue
            if not f.exists():
                continue
            size = f.stat().st_size
            if size < 10240:
                continue
            mb = size / 1024 / 1024
            self.log(f"✅ Готов: {f.name} ({mb:.1f} МБ) — можно монтировать", "success")
            self.set_status(f"✅ {f.name} ({mb:.1f} МБ)")
            self._reported.add(f)


# ═══════════════════════════════════════════════════════════════════════════════
#  StreamRecorder
# ═══════════════════════════════════════════════════════════════════════════════

class StreamRecorder:
    """
    Twitch:
        streamlink --stdout | ffmpeg -f segment -segment_time N -c copy seg_%04d.ts

    Другие платформы:
        yt-dlp -o - URL | ffmpeg -f segment -segment_time N -c copy seg_%04d.mp4

    Режим VOD (только новое для ещё растущего архива):
        Частый поллинг (VOD_POLL_INTERVAL) короткими кусками через открытый
        диапазон "*START-" (yt-dlp сам "хвостит" растущий VOD в пределах его
        короткого DVR-буфера), затем склейка накопленных кусков в файл
        нужного размера (segment_minutes) через ffmpeg -f concat -c copy.

    ffmpeg segment muxer режет по GOP-границам — без разрывов, файлы готовы мгновенно.
    """

    def __init__(
        self, platform, channel, output_dir, segment_minutes,
        twitch_quality, log_cb, status_cb, done_cb, mode="stream"
    ):
        self.platform       = platform
        self.channel        = channel.strip()
        self.output_dir     = output_dir
        self.segment_sec    = segment_minutes * 60
        self.twitch_quality = twitch_quality
        self.log            = log_cb
        self.set_status     = status_cb
        self.done_cb        = done_cb
        self.mode           = mode          # "stream" (ждём эфир) или "vod" (архив/докачка)
        self.debug_mode     = (mode == "vod")  # в режиме VOD включаем подробные debug-принты

        self._stop_event   = threading.Event()
        self._user_stopped = False
        self._src_proc      = None   # streamlink или yt-dlp
        self._ff_proc        = None   # ffmpeg
        self._vod_procs      = set()   # параллельные yt-dlp VOD offset-задачи
        self._watcher        = None
        self.is_running      = False
        self.stream_count    = 0
        self._current_vod_url = None  # если задан - _build_stream_url() отдаёт именно его

        # ── Фикс бесконечного перескачивания одного и того же VOD ───────────
        # id последнего VOD, для которого запись была полностью доведена до
        # конца (обычный докачка целиком ИЛИ докачка "только новое" после
        # роста). Используется в _record_one_stream(), чтобы при автопоиске
        # по нику не начинать качать заново тот же самый архив, если канал
        # всё ещё офлайн и новых архивов не появилось.
        self._last_finished_vod_id = None

    def _dbg(self, msg: str):
        """Пишет в лог отдельной цветной строкой, только если включён Debug-режим."""
        if self.debug_mode:
            self.log(f"🐞 {msg}", "debug")

    def run(self):
        self.is_running = True
        try:
            while True:
                self._record_one_stream()
                if self._user_stopped:
                    break
                if extract_twitch_vod_id(self.channel):
                    # Это конкретный VOD, а не канал — повторного "стрима" не будет
                    break
                # Стрим завершился сам — ждём следующий
                self.stream_count += 1
                self.log(
                    f"📴 Стрим #{self.stream_count} завершён. "
                    f"Ожидаю следующий через {CHECK_INTERVAL}с…", "warn"
                )
                self.set_status(f"⏳ Ожидание стрима #{self.stream_count + 1}…")
                self._stop_event.clear()
                self._stop_event.wait(CHECK_INTERVAL)
        except Exception as e:
            self.log(f"✖ Критическая ошибка: {e}", "error")
        finally:
            self.is_running = False
            self.done_cb()

    def stop(self):
        self._user_stopped = True
        self._stop_event.set()
        self._kill_procs()

    # ── Pipeline: src → pipe → ffmpeg segment ────────────────────────────────
    def _run_pipeline(self, src_cmd: list, src_label: str):
        """
        Запускает src_cmd и передаёт его stdout в ffmpeg -f segment.
        ffmpeg сам режет по GOP и создаёт готовые файлы.
        """
        flags = _subprocess_flags()

        seg_pattern = self._current_seg_pattern

        ff_cmd = [
            "ffmpeg", "-y",
            "-i", "pipe:0",
            "-c", "copy",
            "-f", "segment",
            "-segment_time",       str(self.segment_sec),
            "-segment_time_delta", "1",
            "-reset_timestamps",   "1",
        ]
        # В VOD-режиме Twitch пишем сразу в MP4-сегменты, чтобы каждый
        # закрытый файл можно было открыть/монтировать во время эфира.
        if seg_pattern.lower().endswith(".mp4"):
            ff_cmd += ["-segment_format", "mp4"]
        ff_cmd += [seg_pattern]

        self._dbg(f"CMD [{src_label}]: {' '.join(src_cmd)}")
        self._dbg(f"CMD [ff]: {' '.join(ff_cmd)}")

        try:
            self._src_proc = subprocess.Popen(
                src_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                creationflags=flags,
            )
            self._ff_proc = subprocess.Popen(
                ff_cmd,
                stdin=self._src_proc.stdout,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                creationflags=flags,
            )
            self._src_proc.stdout.close()
            self._dbg(f"PID [{src_label}]={self._src_proc.pid}  PID [ff]={self._ff_proc.pid}")
        except FileNotFoundError as e:
            self.log(
                f"✖ Не найден: {e.filename}. "
                f"Установите streamlink/yt-dlp и ffmpeg.", "error"
            )
            return
        except Exception as e:
            self.log(f"✖ Ошибка запуска: {e}", "error")
            return

        self.log(
            f"🔴 Запись начата → ffmpeg segment "
            f"({self.segment_sec // 60} мин/сегмент)", "success"
        )
        self.set_status(f"🔴 [{self.platform.value}] Запись…")

        def _src_err():
            try:
                for raw in self._src_proc.stderr:
                    line = raw.decode("utf-8", errors="replace").rstrip()
                    if not line:
                        continue
                    if "error" in line.lower():
                        self.log(f"  [{src_label}] {line}", "error")
                    else:
                        self._dbg(f"[{src_label}] {line}")
            except Exception:
                pass
        threading.Thread(target=_src_err, daemon=True).start()

        def _ff_err():
            try:
                for raw in self._ff_proc.stderr:
                    line = raw.decode("utf-8", errors="replace").rstrip()
                    if not line:
                        continue
                    low = line.lower()
                    if "opening" in low and "for writing" in low:
                        m = re.search(r"Opening '(.+?)' for writing", line)
                        if m:
                            fname = Path(m.group(1)).name
                            self.log(f"  ✂ Новый сегмент: {fname}", "info")
                    elif any(k in low for k in ("error", "failed", "invalid")):
                        self.log(f"  [ff] {line}", "warn")
                    else:
                        self._dbg(f"[ff] {line}")
            except Exception:
                pass
        threading.Thread(target=_ff_err, daemon=True).start()

        last_log = time.time()

        while not self._stop_event.is_set():
            rc_src = self._src_proc.poll()
            rc_ff  = self._ff_proc.poll()

            if time.time() - last_log > 30:
                self.log("  ⬛ Запись идёт…", "dim")
                self._dbg(f"tick: src_alive={rc_src is None} ff_alive={rc_ff is None}")
                last_log = time.time()

            if rc_src is not None:
                self.log(f"  {src_label} завершился (код {rc_src})", "dim")
                try:
                    self._ff_proc.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    kill_process_tree(self._ff_proc.pid)
                if not self._user_stopped:
                    self._stop_event.set()
                break

            if rc_ff is not None:
                self.log(f"  ffmpeg завершился (код {rc_ff})", "dim")
                if self._src_proc.poll() is None:
                    kill_process_tree(self._src_proc.pid)
                if not self._user_stopped:
                    self._stop_event.set()
                break

            self._stop_event.wait(2)

        self._kill_procs()
        self.log("⏹ Запись завершена", "info")

    # ── Ожидание стрима ───────────────────────────────────────────────────────
    def _wait_for_stream(self):
        while not self._stop_event.is_set():
            info = self._check_online()
            if info:
                return info
            self.log(
                f"📡 [{self.platform.value}/{self.channel}] "
                f"Офлайн. Проверка через {CHECK_INTERVAL}с…", "info"
            )
            self._stop_event.wait(CHECK_INTERVAL)
        return None

    def _check_online(self):
        if self.platform == Platform.TWITCH:
            login = extract_twitch_channel_login(self.channel) or self.channel.strip()
            self._dbg(f"GQL-запрос статуса Twitch для login='{login}'")
            _, stream = fetch_stream_info_twitch(login)
            self._dbg(f"GQL-ответ: {stream}")
            return stream
        url = self._build_stream_url()
        self._dbg(f"yt-dlp проверка is_live для URL: {url}")
        is_live, title = check_stream_live_ytdlp(url)
        self._dbg(f"yt-dlp ответ: is_live={is_live} title={title!r}")
        return {"title": title, "started_at": None} if is_live else None

    def _build_stream_url(self) -> str:
        # В режиме VOD с автопоиском по нику self.channel — это просто ник,
        # а не ссылка; реальный URL найденного VOD лежит в _current_vod_url.
        if self._current_vod_url:
            return self._current_vod_url
        channel = self.channel.strip()
        if is_url(channel):
            return channel
        return PLATFORM_URL_TEMPLATES.get(
            self.platform, "{channel}"
        ).format(channel=self.channel)

    # ── Итог ──────────────────────────────────────────────────────────────────
    def _print_summary(self, work_dir: Path, glob_pat: str):
        files = sorted(work_dir.glob(glob_pat))
        self.log(f"\n{'═'*50}", "success")
        self.log(f"✅ Запись завершена! Сегментов: {len(files)}", "success")
        total = 0.0
        for f in files:
            if f.exists():
                mb = f.stat().st_size / 1024 / 1024
                total += mb
                self.log(f"   📄 {f.name} ({mb:.1f} МБ)", "success")
        self.log(f"   Итого: {total:.1f} МБ | 📁 {work_dir}", "success")

    def _kill_procs(self):
        """
        Убивает src- и ff-процессы вместе со всеми их потомками.

        Раньше здесь был proc.terminate() — на Windows это убивает ТОЛЬКО
        указанный PID и не трогает дочерние процессы. yt-dlp (и иногда
        streamlink) сам порождает дочерний ffmpeg для мультиплексирования
        HLS-потока, поэтому после "Стоп" родительский yt-dlp умирал,
        а его дочерний ffmpeg продолжал молча качать в фоне.
        Теперь используется kill_process_tree() — taskkill /F /T на Windows
        или killpg на POSIX, убивающий всё дерево процессов целиком.
        """
        procs = [self._src_proc, self._ff_proc]
        procs.extend(list(getattr(self, "_vod_procs", set())))
        for proc in procs:
            if proc and proc.poll() is None:
                kill_process_tree(proc.pid)
                try:
                    proc.wait(timeout=5)
                except Exception:
                    pass
        self._src_proc = None
        self._ff_proc  = None
        getattr(self, "_vod_procs", set()).clear()

    @property
    def _current_seg_pattern(self):
        return getattr(self, "_seg_pattern_store", "")

    # ── Режим VOD: автопоиск + проверка роста ───────────────────────────────
    def _resolve_twitch_vod_auto(self):
        """
        Автопоиск VOD для режима "VOD" на Twitch, когда в поле канала введён
        просто ник (без прямой ссылки на конкретное видео).

        Берём статус "онлайн/оффлайн" и свежий список архивных VOD канала
        через тот же GQL, что уже используется для отслеживания эфира, и
        выбираем самый свежий VOD — если канал сейчас в эфире, это, как
        правило, и есть его текущая, ещё дописываемая трансляция.

        Возвращает dict {"vod_id","url","title","is_online"} или None,
        если канал не найден или у него ещё нет ни одного архивного VOD.
        """
        login = extract_twitch_channel_login(self.channel) or self.channel.strip()
        self._dbg(f"автопоиск VOD: ник='{login}'")

        _, stream = fetch_stream_info_twitch(login)
        is_online = stream is not None
        self._dbg(f"канал онлайн: {is_online}")

        # КРИТИЧЕСКИ ВАЖНО: если стример офлайн, в режиме авто-VOD
        # вообще НЕ запрашиваем список VOD и НЕ проверяем duration/рост VOD.
        # Иначе Twitch может вернуть последний старый архив, после чего код
        # начинает ошибочно воспринимать его как актуальный и скачивать его.
        # Пока канал офлайн нам нужен только один факт: "ждать эфир".
        if not is_online:
            self._dbg("канал офлайн — VOD не проверяю, duration не запрашиваю, жду эфир")
            return None

        # Список VOD нужен только когда канал реально находится в эфире.
        # Тогда последний архив — это текущий растущий VOD, который можно
        # безопасно проверить на рост и докачивать по короткому DVR-интервалу.
        videos = fetch_twitch_videos(login, first=3)
        if not videos:
            self._dbg("канал онлайн, но список архивных VOD пока пуст")
            return None

        latest = videos[0]
        vod_id = latest["id"]
        title  = latest["title"]
        url    = f"https://www.twitch.tv/videos/{vod_id}"
        self._dbg(f"последний VOD: id={vod_id} title={title!r} length={latest['length']}с")

        return {"vod_id": vod_id, "url": url, "title": title, "is_online": is_online}

    def _check_vod_growing(self, vod_url: str) -> tuple:
        """
        Проверяет, растёт ли ещё этот VOD прямо сейчас (эфир идёт и архив
        дописывается), сравнивая доступную длительность через yt-dlp дважды
        с паузой VOD_POLL_INTERVAL между замерами.

        Возвращает (is_growing, known_length):
          is_growing   — True, если между замерами длина заметно выросла
                         (или длина ещё не определяется вовсе — считаем это
                         "растёт", это безопасный дефолт: свежесозданный VOD
                         в первые секунды может не отдавать duration).
          known_length — длина, которая уже точно устоялась и не изменится
                         (замерена ДО ожидания) — с неё начинаем докачку
                         "только нового".
        """
        len1 = fetch_vod_duration_seconds(vod_url)
        if len1 is None:
            self._dbg("длина VOD ещё не определяется — считаю активным/растущим")
            return True, 0.0

        self._dbg(f"проверка роста VOD: сейчас {len1:.0f}с, жду {VOD_POLL_INTERVAL}с…")
        self._stop_event.wait(VOD_POLL_INTERVAL)
        if self._stop_event.is_set():
            return False, len1

        len2 = fetch_vod_duration_seconds(vod_url)
        self._dbg(f"длина после ожидания: {len2}")
        if len2 is not None and len2 > len1 + 1:
            return True, len1
        return False, len1

    def _download_stable_prefix(self, vod_url: str, out_file: Path, end_offset: float, flags) -> bool:
        """
        Разовая докачка уже устоявшейся (гарантированно не растущей) части
        VOD целиком, ОДНИМ файлом, без сегментации — этот кусок больше не
        изменится, дробить его незачем. Выполняется один раз перед тем, как
        включится поллинг "только нового" (_record_vod_new_only) для хвоста,
        который ещё дописывается.

        Точность нарезки здесь не важна — файл всё равно один; главное,
        чтобы он покрывал именно ту часть, что точно не поменяется.
        """
        if end_offset <= 1:
            return False

        end_str = sec_to_hms(int(end_offset))
        self.log(f"⬇ Качаю уже готовую часть VOD (0–{end_str}) одним файлом…", "info")

        yt_cmd = [
            "yt-dlp",
            "--download-sections", f"*0-{end_str}",
            "-f", "bv*+ba/b",
            "--merge-output-format", "mp4",
            "--no-part",
            "--force-overwrites",
            "-o", str(out_file),
            vod_url,
        ]
        self._dbg(f"CMD [stable]: {' '.join(yt_cmd)}")

        try:
            proc = subprocess.Popen(
                yt_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, encoding="utf-8", errors="replace", creationflags=flags,
            )
        except FileNotFoundError as e:
            self.log(f"✖ Не найден: {e.filename}. Установите yt-dlp.", "error")
            return False
        except Exception as e:
            self.log(f"✖ Ошибка запуска докачки стабильной части: {e}", "error")
            return False

        self._src_proc = proc
        self._ff_proc  = None

        def _reader():
            try:
                for raw in proc.stdout:
                    line = raw.rstrip()
                    if not line:
                        continue
                    low = line.lower()
                    if any(k in low for k in ("error", "traceback")):
                        self.log(f"  [stable] {line}", "warn")
                    else:
                        self._dbg(f"[stable] {line}")
            except Exception:
                pass
        threading.Thread(target=_reader, daemon=True).start()

        # ждём завершения, но остаёмся отзывчивыми к "Стоп"
        while proc.poll() is None:
            if self._stop_event.wait(2):
                kill_process_tree(proc.pid)
                break
        self._kill_procs()

        if self._user_stopped:
            return False

        if out_file.exists() and out_file.stat().st_size > 10240:
            mb = out_file.stat().st_size / 1024 / 1024
            self.log(f"✅ Готово: {out_file.name} ({mb:.1f} МБ) — стабильная часть", "success")
            return True

        self.log("⚠ Стабильная часть не скачалась (VOD только начался?) — продолжаю без неё", "warn")
        return False

    def _record_one_stream(self):
        self._current_vod_url = None
        manual_vod_id = extract_twitch_vod_id(self.channel)   # прямая ссылка на конкретный VOD
        auto_info     = None
        vod_id        = None
        vod_title     = None

        if manual_vod_id:
            vod_id = manual_vod_id
            self._current_vod_url = self.channel.strip()
            self.set_status(f"⏳ [{self.platform.value}] Получение информации о VOD…")
            vod_title = fetch_vod_title(self._current_vod_url)

        elif self.platform == Platform.TWITCH and self.mode == "vod":
            # REAL VOD MODE:
            # 1) wait only for ONLINE status;
            # 2) only when ONLINE, resolve the current growing VOD id;
            # 3) download from twitch.tv/videos/<id> via yt-dlp, NEVER via
            #    streamlink/live channel URL. This is important because Twitch
            # VOD has its own VOD audio routing/track.
            self.set_status(f"⏳ [{self.platform.value}] Ожидание ONLINE + текущего VOD…")
            self.log(
                f"📼 VOD-режим: жду ONLINE канала «{self.channel}», затем беру "
                f"именно текущий twitch.tv/videos/<id> — live Streamlink НЕ используется.",
                "info",
            )
            while not self._stop_event.is_set():
                info = self._check_online()
                if not info:
                    self.log(
                        f"📡 [Twitch/{self.channel}] Офлайн. VOD не запрашиваю. "
                        f"Проверка через {CHECK_INTERVAL}с…", "info"
                    )
                    self._stop_event.wait(CHECK_INTERVAL)
                    continue

                login = extract_twitch_channel_login(self.channel) or self.channel.strip()
                videos = fetch_twitch_videos(login, first=3)
                if not videos:
                    self.log(
                        "📼 ONLINE, но текущий VOD ещё не появился в архиве. "
                        f"Повтор через {VOD_POLL_INTERVAL}с…", "warn"
                    )
                    self._stop_event.wait(VOD_POLL_INTERVAL)
                    continue

                candidate = videos[0]
                vod_id = candidate["id"]
                vod_title = candidate["title"]
                self._current_vod_url = f"https://www.twitch.tv/videos/{vod_id}"

                if vod_id == self._last_finished_vod_id:
                    # Same VOD after a transient API refresh: do not restart it.
                    self._dbg(f"текущий VOD {vod_id} уже обработан — жду новый эфир")
                    self._stop_event.wait(VOD_POLL_INTERVAL)
                    continue

                self.log(
                    f"🔴 ONLINE → найден текущий VOD: «{vod_title}» "
                    f"({self._current_vod_url})", "success"
                )
                self.log(
                    "📼 Источник записи: Twitch VOD через yt-dlp. "
                    "Streamlink/live URL в этом режиме не запускается.", "success"
                )
                break

            if self._stop_event.is_set():
                return

        if vod_id:
            stream_info = {"title": vod_title, "started_at": None}
        else:
            self.set_status(f"⏳ [{self.platform.value}] Ожидание стрима…")
            stream_info = self._wait_for_stream()
        if stream_info is None:
            return

        title      = stream_info.get("title", self.channel)
        started_at = stream_info.get("started_at")
        safe_title = safe_filename(title)

        if vod_id:
            folder_name = safe_filename(
                f"{self.platform.value}_VOD{vod_id}_"
                f"{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            )
        else:
            folder_name = safe_filename(
                f"{self.platform.value}_{self.channel}_"
                f"{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            )
        work_dir = self.output_dir / folder_name
        work_dir.mkdir(parents=True, exist_ok=True)

        if started_at:
            elapsed = max(0.0, (datetime.now(timezone.utc) - started_at).total_seconds())
            self.log(
                f"🎬 [{self.platform.value}] «{title}» "
                f"(идёт {int(elapsed//60)}м {int(elapsed%60)}с)", "success"
            )
        elif vod_id:
            self.log(f"📼 [{self.platform.value}] VOD «{title}» (id {vod_id})", "success")
        else:
            self.log(f"🎬 [{self.platform.value}] «{title}»", "success")

        self.log(f"📁 Папка: {work_dir}", "info")
        self.log(
            f"✂ ffmpeg segment muxer: "
            f"{self.segment_sec // 60} мин/сегмент — каждый файл готов сразу", "info"
        )

        ext = "ts" if self.platform == Platform.TWITCH else "mp4"
        if self.platform == Platform.TWITCH and self.mode == "vod" and not manual_vod_id:
            ext = "mp4"

        # REAL VOD MODE: use the VOD URL and poll only the newly appended VOD
        # material. Do not start Streamlink in this branch.
        if self.platform == Platform.TWITCH and self.mode == "vod" and vod_id:
            ext = "mp4"
            self._record_vod_new_only(work_dir, safe_title, ext, initial_offset=None)
            glob_pat = f"{glob_mod.escape(safe_title)}_*.{ext}"
            self._print_summary(work_dir, glob_pat)
            if not self._user_stopped:
                self._last_finished_vod_id = vod_id
            return

        use_new_only = False
        initial_off = None

        self._seg_pattern_store = str(work_dir / f"{safe_title}_%02d.{ext}")

        self._watcher = SegmentWatcher(
            work_dir=work_dir, glob_pattern=glob_pat,
            log_cb=self.log, status_cb=self.set_status,
            stop_event=self._stop_event,
        )
        self._watcher.start()

        stream_url = self._build_stream_url()

        if self.platform == Platform.TWITCH:
            self._run_pipeline(
                src_cmd=[
                    "streamlink",
                    "--loglevel", "warning",
                    "--stream-segment-attempts", "10",
                    "--stream-segment-timeout",  "15",
                    "--stream-timeout",          "120",
                    "--ringbuffer-size",         "64M",
                    "--stdout",
                    stream_url,
                    self.twitch_quality,
                ],
                src_label="sl",
            )
        else:
            self._run_pipeline(
                src_cmd=[
                    "yt-dlp",
                    "-o", "-",
                    "--no-part",
                    "--hls-use-mpegts",
                    "-f", "best[ext=mp4]/best",
                    "--live-from-start",
                    stream_url,
                ],
                src_label="yt",
            )

        self._watcher.join_done(timeout=15)
        self._print_summary(work_dir, glob_pat)

        # запись обычным конвейером (streamlink/yt-dlp → ffmpeg segment)
        # тоже доведена до конца — если это был VOD, запоминаем его id,
        # чтобы автопоиск не начал качать его же заново на следующем витке
        if vod_id and not self._user_stopped:
            self._last_finished_vod_id = vod_id

    # ── VOD: параллельная загрузка фиксированных offset-сегментов ─────────────
    def _download_vod_offset(self, stream_url, out_file: Path, start_sec: int,
                             end_sec: int, flags) -> bool:
        """Скачать РОВНО один фиксированный VOD-диапазон.

        Никаких открытых диапазонов, 10/20-секундного поллинга, ffprobe или
        локальной склейки. Если размер сегмента = 60 сек, команда получает
        именно *0:00-1:00, затем *1:00-2:00 и т.д.
        """
        start_str = sec_to_hms(start_sec)
        end_str = sec_to_hms(end_sec)
        self._dbg(f"offset {start_str}-{end_str} → {out_file.name}")

        yt_cmd = [
            "yt-dlp",
            "--download-sections", f"*{start_str}-{end_str}",
            "-f", "bv*+ba/b",
            "--merge-output-format", "mp4",
            "--no-part",
            "--force-overwrites",
            "--socket-timeout", "15",
            "-o", str(out_file),
            stream_url,
        ]
        self._dbg(f"CMD [yt-offset]: {' '.join(yt_cmd)}")

        try:
            proc = subprocess.Popen(
                yt_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, encoding="utf-8", errors="replace", creationflags=flags
            )
            self._vod_procs.add(proc)
        except FileNotFoundError as e:
            self.log(f"✖ Не найден: {e.filename}. Установите yt-dlp.", "error")
            return False
        except Exception as e:
            self.log(f"✖ Ошибка запуска offset {start_str}-{end_str}: {e}", "error")
            return False

        try:
            for raw in proc.stdout:
                line = raw.rstrip()
                if not line:
                    continue
                low = line.lower()
                if any(k in low for k in ("error", "traceback", "403", "wrapped")):
                    self.log(f"  [offset {start_str}] {line}", "warn")
                elif self.debug_mode:
                    self._dbg(f"[offset {start_str}] {line}")
        except Exception:
            pass
        finally:
            try:
                proc.wait(timeout=10)
            except Exception:
                kill_process_tree(proc.pid)
            self._vod_procs.discard(proc)

        if self._user_stopped:
            return False
        if proc.returncode == 0 and out_file.exists() and out_file.stat().st_size > 10240:
            mb = out_file.stat().st_size / 1024 / 1024
            self.log(
                f"✅ Offset {start_str}–{end_str} готов: {out_file.name} ({mb:.1f} МБ) — можно монтировать",
                "success",
            )
            return True

        try:
            out_file.unlink(missing_ok=True)
        except Exception:
            pass
        self.log(f"⚠ Offset {start_str}–{end_str} не скачан (код {proc.returncode})", "warn")
        return False

    def _record_vod_new_only(self, work_dir: Path, safe_title: str, ext: str, initial_offset=None):
        """VOD-мониторинг с фиксированными offset-сегментами и параллельной загрузкой.

        Главный принцип:
          segment_sec = 60  ->  [0,60], [60,120], [120,180] ...

        Все уже полностью доступные интервалы ставятся в очередь ОДНОВРЕМЕННО.
        Никаких промежуточных 10/20-секундных файлов и никакой concat-склейки.
        Последняя неполная минута просто ждёт, пока VOD дорастёт до её конца.
        """
        stream_url = self._current_vod_url or self._build_stream_url()
        flags = _subprocess_flags()
        seg = max(1, int(self.segment_sec))

        # С какой минуты начинаем. Для обычного запуска 0:00 означает скачать
        # весь уже доступный VOD. initial_offset оставлен для совместимости.
        start = max(0, int(initial_offset or 0))
        start = (start // seg) * seg

        self.log(
            f"⚡ VOD OFFSET-режим: сегмент {seg}с. "
            f"Качаю фиксированные диапазоны {sec_to_hms(start)}-{sec_to_hms(start+seg)}, "
            f"{sec_to_hms(start+seg)}-{sec_to_hms(start+2*seg)} … ПАРАЛЛЕЛЬНО.",
            "success",
        )
        self.log(
            "🚫 Нет 10/20-секундных докачек, нет ffprobe каждого куска, нет concat. "
            "Каждый offset = отдельный готовый MP4.", "info"
        )
        self.set_status(f"🔴 [{self.platform.value}] Параллельная загрузка VOD offset-сегментов…")

        # Уже существующие финальные файлы не скачиваем повторно.
        existing = set(work_dir.glob(f"{safe_title}_*.{ext}"))
        scheduled = set()
        completed = set()
        for f in existing:
            m = re.search(r"_(\d{4,})\.(?:mp4|ts)$", f.name)
            if m:
                completed.add(int(m.group(1)))

        # Число параллельных VOD-запросов. Для Windows/Twitch разумный default;
        # UI всё равно остаётся отзывчивым.
        workers = max(2, min(8, getattr(self, "vod_parallel", 6)))
        executor = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="vod-offset")
        futures = {}

        try:
            while not self._stop_event.is_set():
                if self.platform == Platform.TWITCH and not self._check_online():
                    # Если эфир уже закончился, один раз получаем финальную длину
                    # и добиваем последние полные offset-минуты.
                    self.log("📴 Эфир завершён — получаю финальную длину VOD и добиваю offset-очередь.", "info")
                    final_len = fetch_vod_duration_seconds(stream_url) or 0
                    target = int(final_len // seg) * seg
                else:
                    current_len = fetch_vod_duration_seconds(stream_url)
                    if current_len is None:
                        self._stop_event.wait(5)
                        continue
                    # Последнюю неполную минуту НЕ качаем: ждём полного end_offset.
                    target = int(current_len // seg) * seg

                # Ставим в очередь все полные сегменты до target. Один offset
                # запускается один раз и не дробится на подзапросы.
                start_idx = start // seg
                end_idx = target // seg
                for idx in range(start_idx, end_idx):
                    if idx in completed or idx in scheduled:
                        continue
                    a = idx * seg
                    b = a + seg
                    out_file = work_dir / f"{safe_title}_{idx:04d}.{ext}"
                    if out_file.exists() and out_file.stat().st_size > 10240:
                        completed.add(idx)
                        continue
                    scheduled.add(idx)
                    fut = executor.submit(self._download_vod_offset, stream_url, out_file, a, b, flags)
                    futures[fut] = idx

                # Забираем результаты уже закончившихся задач, не блокируя
                # очередь новых offset-минут.
                done = [f for f in futures if f.done()]
                for fut in done:
                    idx = futures.pop(fut)
                    try:
                        ok = fut.result()
                    except Exception as e:
                        ok = False
                        self.log(f"⚠ Offset {idx} упал: {e}", "warn")
                    if ok:
                        completed.add(idx)
                    else:
                        scheduled.discard(idx)

                self.set_status(
                    f"🔴 [{self.platform.value}] VOD: готово {len(completed)} | "
                    f"в очереди {len(futures)} | параллельно до {workers}"
                )

                # Если канал offline, после постановки финальной очереди ждём,
                # пока все уже запущенные offset-загрузки закончатся.
                if self.platform == Platform.TWITCH and not self._check_online():
                    if not futures:
                        break
                    self._stop_event.wait(1)
                else:
                    self._stop_event.wait(VOD_POLL_INTERVAL)

        finally:
            if self._user_stopped:
                for proc in list(self._vod_procs):
                    if proc.poll() is None:
                        kill_process_tree(proc.pid)
            # Не оставляем executor с живыми рабочими потоками.
            executor.shutdown(wait=not self._user_stopped, cancel_futures=self._user_stopped)
            self._vod_procs.clear()

        self.log(
            f"⏹ VOD offset-загрузка остановлена. Готовых сегментов: {len(completed)}. "
            f"Каждый файл — отдельный фиксированный {seg}-секундный диапазон.",
            "success" if completed else "warn",
        )


# ═══════════════════════════════════════════════════════════════════════════════
#  StreamSession — UI-карточка
# ═══════════════════════════════════════════════════════════════════════════════

class StreamSession:
    def __init__(self, parent_frame, app, session_id: int, on_remove):
        self.app         = app
        self.session_id  = session_id
        self.on_remove   = on_remove
        self._recorder   = None
        self._thread     = None
        self._log_queue  = queue.Queue()
        self._status_var = tk.StringVar(value="Готов")
        self._build(parent_frame)
        self._drain_loop()

    def _build(self, parent):
        self.card = ctk.CTkFrame(
            parent, fg_color=BG_CARD,
            corner_radius=12, border_width=1, border_color=BORDER
        )
        self.card.pack(fill="x", padx=0, pady=(0, 10))

        hdr = ctk.CTkFrame(self.card, fg_color=BG_PANEL, corner_radius=8)
        hdr.pack(fill="x", padx=8, pady=(8, 4))

        self._title_label = ctk.CTkLabel(
            hdr, text=f"🎮 Стример #{self.session_id}",
            font=ctk.CTkFont(size=13, weight="bold"), text_color=PURPLE_LT
        )
        self._title_label.pack(side="left", padx=10, pady=6)

        self._status_label = ctk.CTkLabel(
            hdr, textvariable=self._status_var,
            font=ctk.CTkFont(size=11), text_color=YELLOW
        )
        self._status_label.pack(side="left", padx=8)

        self._remove_btn = ctk.CTkButton(
            hdr, text="✕", width=28, height=28,
            fg_color="#3f1515", hover_color=RED,
            font=ctk.CTkFont(size=12, weight="bold"),
            command=self._on_remove_click
        )
        self._remove_btn.pack(side="right", padx=8, pady=4)

        form = ctk.CTkFrame(self.card, fg_color="transparent")
        form.pack(fill="x", padx=8, pady=4)
        form.columnconfigure(1, weight=1)
        form.columnconfigure(3, weight=1)

        ctk.CTkLabel(form, text="Платформа:", text_color=GRAY_LT,
                     font=ctk.CTkFont(size=12)
                     ).grid(row=0, column=0, padx=(8,4), pady=6, sticky="w")

        platforms = [f"{PLATFORM_ICONS[p]} {p.value}" for p in Platform]
        self._platform_var = tk.StringVar(value=platforms[0])
        self._platform_menu = ctk.CTkOptionMenu(
            form, values=platforms, variable=self._platform_var,
            font=ctk.CTkFont(size=12), fg_color=BG_HOVER,
            button_color=PURPLE, button_hover_color=PURPLE_DK,
            width=160, height=32, command=self._on_platform_change
        )
        self._platform_menu.grid(row=0, column=1, padx=4, pady=6, sticky="w")

        ctk.CTkLabel(form, text="Канал / URL:", text_color=GRAY_LT,
                     font=ctk.CTkFont(size=12)
                     ).grid(row=0, column=2, padx=(16,4), pady=6, sticky="w")

        self._channel_var = tk.StringVar()
        self._channel_entry = ctk.CTkEntry(
            form, textvariable=self._channel_var,
            placeholder_text="ник или полный URL",
            font=ctk.CTkFont(size=12), height=32
        )
        self._channel_entry.grid(row=0, column=3, padx=(0,8), pady=6, sticky="ew")
        bind_paste_fix(self._channel_entry, self._channel_var)

        self._url_hint = ctk.CTkLabel(
            form, text="https://www.twitch.tv/{канал}",
            font=ctk.CTkFont(size=10), text_color=GRAY
        )
        self._url_hint.grid(row=1, column=1, columnspan=3, padx=(4,8), pady=(0,4), sticky="w")

        btn_row = ctk.CTkFrame(self.card, fg_color="transparent")
        btn_row.pack(fill="x", padx=8, pady=(2, 6))

        self._start_btn = ctk.CTkButton(
            btn_row, text="▶ Старт",
            font=ctk.CTkFont(size=12, weight="bold"),
            fg_color=PURPLE, hover_color=PURPLE_DK,
            height=32, width=100, corner_radius=8,
            command=self._start
        )
        self._start_btn.pack(side="left", padx=(0, 6))

        self._stop_btn = ctk.CTkButton(
            btn_row, text="⏹ Стоп",
            font=ctk.CTkFont(size=12, weight="bold"),
            fg_color="#7f1d1d", hover_color=RED,
            height=32, width=100, corner_radius=8,
            state="disabled", command=self._stop
        )
        self._stop_btn.pack(side="left", padx=(0, 6))

        ctk.CTkButton(
            btn_row, text="🗑 Очистить",
            font=ctk.CTkFont(size=11), fg_color=BG_HOVER, hover_color=BG_HOVER,
            height=32, width=90, corner_radius=8, command=self._clear_log
        ).pack(side="left")

        self._copy_btn = ctk.CTkButton(
            btn_row, text="📋 Копировать лог",
            font=ctk.CTkFont(size=11), fg_color=BG_HOVER, hover_color=PURPLE_DK,
            height=32, width=130, corner_radius=8, command=self._copy_log
        )
        self._copy_btn.pack(side="left", padx=(6, 0))

        ctk.CTkLabel(
            btn_row, text="Режим:", text_color=GRAY_LT,
            font=ctk.CTkFont(size=11)
        ).pack(side="left", padx=(14, 4))

        self._mode_var = tk.StringVar(value="🔴 Поток")
        self._mode_menu = ctk.CTkOptionMenu(
            btn_row, values=["🔴 Поток", "📼 VOD"], variable=self._mode_var,
            font=ctk.CTkFont(size=11), fg_color=BG_HOVER,
            button_color=PURPLE, button_hover_color=PURPLE_DK,
            width=110, height=30, command=self._on_mode_change,
        )
        self._mode_menu.pack(side="left")

        ctk.CTkLabel(
            btn_row, text="ffmpeg -f segment  —  каждый файл готов сразу",
            font=ctk.CTkFont(size=10), text_color=TEAL
        ).pack(side="right", padx=8)

        log_frame = ctk.CTkFrame(self.card, fg_color=BG_DEEP, corner_radius=8)
        log_frame.pack(fill="x", padx=8, pady=(0, 8))

        vsb = tk.Scrollbar(log_frame, orient="vertical", bg=BG_PANEL,
                           troughcolor=BG_DEEP, activebackground=PURPLE)
        vsb.pack(side="right", fill="y")

        self.log_text = tk.Text(
            log_frame, bg=BG_DEEP, fg=WHITE,
            font=("Consolas", 10), bd=0, relief="flat",
            wrap="word", state="disabled", height=8,
            yscrollcommand=vsb.set,
            selectbackground=PURPLE_DK, selectforeground=WHITE,
        )
        self.log_text.pack(side="left", fill="both", expand=True, padx=4, pady=4)
        vsb.config(command=self.log_text.yview)

        for tag, color in [
            ("info", WHITE), ("success", GREEN), ("warn", YELLOW),
            ("error", RED), ("dim", GRAY_LT), ("time", "#4b5563"),
            ("debug", "#38bdf8"),
        ]:
            self.log_text.tag_config(tag, foreground=color)

        menu = tk.Menu(self.card, tearoff=0, bg=BG_CARD, fg=WHITE,
                       activebackground=PURPLE, activeforeground=WHITE, bd=0)
        menu.add_command(label="Копировать всё", command=self._copy_log)
        menu.add_command(label="Очистить",       command=self._clear_log)
        self.log_text.bind(
            "<Button-3>",
            lambda e: (menu.tk_popup(e.x_root, e.y_root), menu.grab_release())
        )
        self._on_platform_change(self._platform_var.get())

    def _on_platform_change(self, value: str):
        self._on_mode_change(self._mode_var.get())

    def _on_mode_change(self, value: str):
        plat = self._get_platform()
        color = PLATFORM_COLORS.get(plat, PURPLE)
        tmpl  = PLATFORM_URL_TEMPLATES.get(plat, "{channel}")
        is_vod_mode = "VOD" in value

        if is_vod_mode and plat == Platform.TWITCH:
            hint = ("<ник> — VOD найдётся автоматически (по последнему архиву канала)"
                    "   или вставьте готовую ссылку twitch.tv/videos/<id>")
        elif is_vod_mode:
            hint = "нужна прямая ссылка на VOD/видео — автопоиск по нику доступен только для Twitch"
        elif plat == Platform.TWITCH:
            hint = (tmpl.replace("{channel}", "<ник>")
                    + "   или   twitch.tv/videos/<id> (VOD)   —   можно вставить готовую ссылку целиком")
        elif plat == Platform.CUSTOM:
            hint = "любая ссылка, которую понимает yt-dlp"
        else:
            hint = tmpl.replace("{channel}", "<ник>") + "   —   можно вставить готовую ссылку целиком"

        self._url_hint.configure(text=hint, text_color=color)

        if is_vod_mode and plat == Platform.TWITCH:
            placeholder = "ник стримера (авто-VOD) или ссылка twitch.tv/videos/<id>"
        elif is_vod_mode:
            placeholder = "ссылка на VOD/видео"
        elif plat == Platform.CUSTOM:
            placeholder = "https://example.com/stream"
        else:
            placeholder = "ник стримера или вставьте ссылку (в т.ч. VOD)"
        self._channel_entry.configure(placeholder_text=placeholder)

    def _get_platform(self) -> Platform:
        val = self._platform_var.get()
        for p in Platform:
            if p.value in val:
                return p
        return Platform.TWITCH

    def _start(self):
        channel = self._channel_var.get().strip()
        if not channel:
            self._log("✖ Введите ник или URL!", "error")
            return
        platform = self._get_platform()
        mode     = "vod" if "VOD" in self._mode_var.get() else "stream"

        if mode == "vod" and platform != Platform.TWITCH and not is_url(channel):
            self._log(
                "✖ Для этой платформы в режиме VOD нужна прямая ссылка на "
                "видео — авто-поиск по нику поддержан только для Twitch", "error"
            )
            return

        out_dir        = Path(self.app.output_var.get().strip())
        seg_min        = int(self.app.seg_min_var.get())
        twitch_quality = self.app.quality_var.get().strip() or "best"
        out_dir.mkdir(parents=True, exist_ok=True)

        self._set_controls_active(False)
        self._clear_log()
        self._title_label.configure(
            text=f"{PLATFORM_ICONS.get(platform, '🌐')} {platform.value} / {channel}"
        )
        self._log(f"🚀 Запуск: {platform.value} / {channel}", "success")
        self._log(f"📁 Папка: {out_dir}", "info")
        if platform == Platform.TWITCH:
            self._log(f"🎵 Twitch CDN — оригинальный звук (качество: {twitch_quality})", "success")

        if mode == "vod":
            if platform == Platform.TWITCH and extract_twitch_vod_id(channel):
                self._log(
                    "📼 Режим VOD: указана прямая ссылка — проверю, растёт ли "
                    "архив ещё, и докачаю его до конца", "warn"
                )
            elif platform == Platform.TWITCH:
                self._log(
                    f"📼 Режим VOD: жду ONLINE канала «{channel}» и пишу live-поток "
                    "сразу сегментами", "warn"
                )
            else:
                self._log("📼 Режим VOD: докачаю видео целиком, проверю, растёт ли ещё", "warn")
            self._log(
                "ℹ В VOD-режиме по нику архивы не опрашиваются: при ONLINE "
                "сразу запускается live-поток, а ffmpeg закрывает сегменты "
                "нужного размера прямо во время эфира.", "dim"
            )

        self._recorder = StreamRecorder(
            platform        = platform,
            channel         = channel,
            output_dir      = out_dir,
            segment_minutes = seg_min,
            twitch_quality  = twitch_quality,
            log_cb          = lambda msg, lvl="info": self._log_queue.put((msg, lvl)),
            status_cb       = lambda msg: self._log_queue.put(("__status__", msg)),
            done_cb         = self._on_done,
            mode            = mode,
        )
        self._thread = threading.Thread(target=self._recorder.run, daemon=True)
        self._thread.start()

    def _stop(self):
        if self._recorder:
            self._log("⏹ Остановка…", "warn")
            self._stop_btn.configure(state="disabled", text="⏳…")
            threading.Thread(target=self._recorder.stop, daemon=True).start()

    def _on_done(self):
        self.card.after(0, self._reset_ui)

    def _reset_ui(self):
        self._set_controls_active(True)
        self._status_var.set("✅ Завершено")

    def _set_controls_active(self, active: bool):
        state = "normal" if active else "disabled"
        self._start_btn.configure(state=state)
        self._stop_btn.configure(
            state="disabled" if active else "normal", text="⏹ Стоп"
        )
        self._platform_menu.configure(state=state)
        self._channel_entry.configure(state=state)
        self._remove_btn.configure(state=state)
        self._mode_menu.configure(state=state)

    def _on_remove_click(self):
        if self._recorder and self._recorder.is_running:
            if not messagebox.askyesno("Удалить", "Запись идёт! Остановить и удалить блок?"):
                return
            self._recorder.stop()
        self.on_remove(self)

    def get_state(self) -> dict:
        return {
            "platform": self._get_platform().value,
            "channel":  self._channel_var.get().strip(),
            "mode":     "vod" if "VOD" in self._mode_var.get() else "stream",
        }

    def restore_state(self, state: dict):
        for p in Platform:
            if p.value == state.get("platform", "Twitch"):
                self._platform_var.set(f"{PLATFORM_ICONS[p]} {p.value}")
                break
        self._mode_var.set("📼 VOD" if state.get("mode") == "vod" else "🔴 Поток")
        self._on_platform_change(self._platform_var.get())
        self._channel_var.set(state.get("channel", ""))

    def destroy(self):
        if self._recorder:
            self._recorder.stop()
        self.card.destroy()

    def _drain_loop(self):
        try:
            while True:
                msg, lvl = self._log_queue.get_nowait()
                if msg == "__status__":
                    self._status_var.set(lvl)
                else:
                    self._append_log(msg, lvl)
        except queue.Empty:
            pass
        self.card.after(100, self._drain_loop)

    def _log(self, msg: str, level: str = "info"):
        self._log_queue.put((msg, level))

    def _append_log(self, msg: str, level: str):
        self.log_text.configure(state="normal")
        ts = datetime.now().strftime("%H:%M:%S")
        self.log_text.insert("end", f"[{ts}] ", "time")
        self.log_text.insert("end", msg + "\n", level)
        self.log_text.see("end")
        self.log_text.configure(state="disabled")

    def _clear_log(self):
        self.log_text.configure(state="normal")
        self.log_text.delete("1.0", "end")
        self.log_text.configure(state="disabled")

    def _copy_log(self):
        text = self.log_text.get("1.0", "end").strip()
        if not text:
            self._flash_copy_btn("Лог пуст")
            return
        try:
            self.card.clipboard_clear()
            self.card.clipboard_append(text)
            self.card.update()
            self._flash_copy_btn("✅ Скопировано")
        except Exception:
            self._flash_copy_btn("✖ Ошибка")

    def _flash_copy_btn(self, text: str):
        original = "📋 Копировать лог"
        self._copy_btn.configure(text=text)
        self.card.after(1400, lambda: self._copy_btn.configure(text=original))


# ═══════════════════════════════════════════════════════════════════════════════
#  Главное окно
# ═══════════════════════════════════════════════════════════════════════════════

class App(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("🔴 Multi-Platform Live Recorder")
        self.geometry("1020x820")
        self.configure(fg_color=BG_DEEP)
        self.resizable(True, True)
        self.cfg = load_config()
        self._sessions = []
        self._session_counter = 0
        self._build_ui()
        saved = self.cfg.get("sessions", [])
        for preset in saved:
            self._add_session(preset)
        if not self._sessions:
            self._add_session()

    def _build_ui(self):
        hdr = ctk.CTkFrame(self, fg_color=BG_PANEL, corner_radius=0)
        hdr.pack(fill="x")

        ctk.CTkLabel(
            hdr, text="🔴  MULTI-PLATFORM LIVE RECORDER",
            font=ctk.CTkFont(size=18, weight="bold"), text_color=PURPLE_LT
        ).pack(side="left", padx=20, pady=10)

        ctk.CTkLabel(
            hdr, text="Twitch · YouTube · Kick · Rumble · W.tv · Wasd.tv",
            font=ctk.CTkFont(size=11), text_color=GRAY_LT
        ).pack(side="left", padx=4)

        ctk.CTkButton(
            hdr, text="➕ Добавить стримера",
            font=ctk.CTkFont(size=12, weight="bold"),
            fg_color=GREEN, hover_color="#059669",
            height=34, corner_radius=8, width=180,
            command=self._add_session
        ).pack(side="right", padx=16, pady=8)

        settings = ctk.CTkFrame(self, fg_color=BG_PANEL, corner_radius=0)
        settings.pack(fill="x")

        row1 = ctk.CTkFrame(settings, fg_color="transparent")
        row1.pack(fill="x", padx=16, pady=(8, 2))

        ctk.CTkLabel(row1, text="Папка:", text_color=GRAY_LT,
                     font=ctk.CTkFont(size=12)).pack(side="left", padx=(0, 6))
        self.output_var = tk.StringVar(
            value=self.cfg.get("output_path", str(Path.home() / "Downloads"))
        )
        output_entry = ctk.CTkEntry(row1, textvariable=self.output_var,
                     font=ctk.CTkFont(size=11), height=30, width=280
                     )
        output_entry.pack(side="left", padx=(0, 4))
        bind_paste_fix(output_entry, self.output_var)
        ctk.CTkButton(row1, text="📂", width=32, height=30,
                      fg_color=BG_HOVER, hover_color=PURPLE_DK,
                      command=self._browse_folder).pack(side="left", padx=(0, 16))

        ctk.CTkLabel(row1, text="Сегмент (мин):", text_color=GRAY_LT,
                     font=ctk.CTkFont(size=12)).pack(side="left", padx=(0, 6))
        self.seg_min_var = tk.StringVar(
            value=str(self.cfg.get("segment_minutes", 30))
        )
        ctk.CTkEntry(row1, textvariable=self.seg_min_var,
                     font=ctk.CTkFont(size=12), height=30, width=52
                     ).pack(side="left", padx=(0, 16))

        ctk.CTkLabel(row1, text="Качество Twitch:", text_color=GRAY_LT,
                     font=ctk.CTkFont(size=12)).pack(side="left", padx=(0, 6))
        self.quality_var = tk.StringVar(value=self.cfg.get("twitch_quality", "best"))
        ctk.CTkOptionMenu(
            row1,
            values=["best", "1080p60", "1080p", "720p60", "720p", "480p", "360p", "worst"],
            variable=self.quality_var,
            font=ctk.CTkFont(size=12), fg_color=BG_HOVER,
            button_color=PURPLE, button_hover_color=PURPLE_DK,
            width=110, height=30,
        ).pack(side="left")

        row2 = ctk.CTkFrame(settings, fg_color="transparent")
        row2.pack(fill="x", padx=16, pady=(0, 8))
        ctk.CTkButton(
            row2, text="💾 Сохранить настройки",
            font=ctk.CTkFont(size=11), fg_color=BG_HOVER, hover_color=BG_HOVER,
            height=28, width=160, corner_radius=6, command=self._save_settings
        ).pack(side="right")

        info = ctk.CTkFrame(self, fg_color="#070d14", corner_radius=0)
        info.pack(fill="x")
        ctk.CTkLabel(
            info,
            text=(
                "✂ streamlink/yt-dlp → pipe → ffmpeg -f segment -c copy  ·  "
                "нарезка по GOP без остановки потока  ·  "
                "каждый закрытый файл сразу готов к монтажу"
            ),
            font=ctk.CTkFont(size=11), text_color=TEAL, anchor="w"
        ).pack(fill="x", padx=16, pady=5)

        self._scroll_frame = ctk.CTkScrollableFrame(
            self, fg_color=BG_DEEP, corner_radius=0,
            scrollbar_button_color=PURPLE, scrollbar_button_hover_color=PURPLE_LT
        )
        self._scroll_frame.pack(fill="both", expand=True)

    def _add_session(self, preset: dict = None):
        self._session_counter += 1
        sess = StreamSession(
            parent_frame=self._scroll_frame, app=self,
            session_id=self._session_counter, on_remove=self._remove_session,
        )
        if preset:
            sess.restore_state(preset)
        self._sessions.append(sess)

    def _remove_session(self, sess):
        if sess in self._sessions:
            self._sessions.remove(sess)
        sess.destroy()

    def _browse_folder(self):
        d = filedialog.askdirectory(title="Папка для сохранения")
        if d:
            self.output_var.set(d)

    def _save_settings(self):
        try:
            seg = int(self.seg_min_var.get())
            assert 1 <= seg <= 360
        except Exception:
            messagebox.showerror("Ошибка", "Размер сегмента: целое число от 1 до 360")
            return
        sessions_state = [s.get_state() for s in self._sessions]
        self.cfg.update({
            "output_path":     self.output_var.get(),
            "segment_minutes": seg,
            "twitch_quality":  self.quality_var.get(),
            "sessions":        sessions_state,
        })
        save_config(self.cfg)
        messagebox.showinfo("Сохранено", f"Настройки сохранены!\nСтримеров: {len(sessions_state)}")

    def on_closing(self):
        active = [s for s in self._sessions if s._recorder and s._recorder.is_running]
        if active:
            if not messagebox.askyesno(
                "Выход", f"Идёт запись {len(active)} стримера(ов). Остановить и выйти?"
            ):
                return
        try:
            seg = int(self.seg_min_var.get())
        except Exception:
            seg = 30
        self.cfg.update({
            "output_path":     self.output_var.get(),
            "segment_minutes": seg,
            "twitch_quality":  self.quality_var.get(),
            "sessions":        [s.get_state() for s in self._sessions],
        })
        save_config(self.cfg)
        for s in self._sessions:
            if s._recorder:
                s._recorder.stop()
        self.destroy()


if __name__ == "__main__":
    app = App()
    app.protocol("WM_DELETE_WINDOW", app.on_closing)
    app.mainloop()