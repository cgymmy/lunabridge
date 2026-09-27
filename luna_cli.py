# -*- coding: utf-8 -*-
"""luna_cli.py — 本地命令行 <-> JumpServer Luna 网页终端 交互桥

原理:
  - 输出: 通过 CDP 捕获 koko/connect iframe 中终端 WS 的下行帧
          (二进制帧直接携带终端输出), 增量 UTF-8 解码还原输出流.
  - 输入: 把命令输入到 xterm.js 的隐藏文本域 (.xterm-helper-textarea),
          用 keyboard.type 产生真实键盘事件, 走 Luna 原生输入通道.
  - 会话: 首次 --login 手动登录一次 (验证码/MFA 无法自动绕过), 保存
          storage_state.json 之后复用; 过期时自动回到登录页等你重新登录.
  - 多标签页: 登录成功后终端可能在新标签页打开, 脚本监控上下文里所有页面.
  - 重要: 等待必须用 page.wait_for_timeout, 不能用 time.sleep —
          同步 API 中 time.sleep 会阻塞事件分发, WS 帧会延迟数分钟.

用法:
  python luna_cli.py --login            # 首次: 弹出浏览器, 手动登录, 保存会话后退出
  python luna_cli.py -c "ls -la"        # 单条命令: 执行后打印输出并退出
  python luna_cli.py                    # 交互 REPL: 本地输入命令, 实时回显输出
  python luna_cli.py --headless         # 隐藏浏览器窗口 (配合 -c 使用)
  python luna_cli.py --wait 2 -c "cmd"  # 单条命令输出静默等待时长(秒, 默认 1.2)
"""
import argparse
import base64
import hashlib
import json
import os
import queue
import re
import shlex
import sys
import tempfile
import threading
import time
import urllib.parse
import uuid

from playwright.sync_api import sync_playwright

# ---------- 配置 ----------
HERE = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.environ.get(
    "LUNA_CONFIG_FILE", os.path.join(HERE, "luna_config.json")
)
try:
    with open(CONFIG_FILE, "r", encoding="utf-8") as config_stream:
        _CONFIG = json.load(config_stream)
except FileNotFoundError:
    _CONFIG = {}
except (OSError, ValueError) as exc:
    print(f"[!] 无法读取 Luna 配置 {CONFIG_FILE}: {exc}", file=sys.stderr)
    _CONFIG = {}

BASE_URL = os.environ.get(
    "LUNA_BASE_URL", str(_CONFIG.get("base_url", ""))
).rstrip("/")
ASSET_ID = os.environ.get(
    "LUNA_ASSET_ID", str(_CONFIG.get("asset_id", ""))
)
ORG_ID = os.environ.get(
    "LUNA_ORG_ID", str(_CONFIG.get("org_id", ""))
)
_sftp_nav = os.environ.get("LUNA_SFTP_NAV")
if _sftp_nav:
    SFTP_NAV = tuple(part.strip() for part in _sftp_nav.split(",")
                     if part.strip())
else:
    SFTP_NAV = tuple(_CONFIG.get("sftp_nav", ()))

if BASE_URL and ASSET_ID and ORG_ID:
    _terminal_path = f"/luna/?login_to={ASSET_ID}&oid={ORG_ID}"
    URL = f"{BASE_URL}{_terminal_path}"
    # 登录成功后 JumpServer 核心会按 next 参数跳回终端。
    LOGIN_URL = (f"{BASE_URL}/core/auth/login/?next="
                 + urllib.parse.quote(_terminal_path, safe=""))
    SFTP_URL = f"{BASE_URL}/koko/elfinder/sftp/"
else:
    URL = ""
    LOGIN_URL = ""
    SFTP_URL = ""

STATE_FILE = os.path.join(HERE, "storage_state.json")
LOG_FILE = os.path.join(HERE, "luna_term.log")
LOG_MAX_BYTES = 20 * 1024 * 1024
LOG_BACKUP_COUNT = 2
DEFAULT_LOG_ENABLED = True
# 目标资产在 JumpServer 文件管理器中的导航路径；资产根目录映射到 /tmp。
SFTP_REMOTE_ROOT = "/tmp"


def validate_config() -> bool:
    missing = []
    if not BASE_URL:
        missing.append("base_url / LUNA_BASE_URL")
    if not ASSET_ID:
        missing.append("asset_id / LUNA_ASSET_ID")
    if not ORG_ID:
        missing.append("org_id / LUNA_ORG_ID")
    if not SFTP_NAV:
        missing.append("sftp_nav / LUNA_SFTP_NAV")
    if missing:
        print("[x] Luna 配置不完整: " + ", ".join(missing))
        print(f"    请复制 luna_config.example.json 为 {CONFIG_FILE} 并填写。")
        return False
    return True


# ---------- 输出收集 ----------
class TermSink:
    """线程安全地收集终端输出, 写日志, 可选实时回显."""

    def __init__(self, echo: bool = False, log_enabled=None):
        self._lock = threading.Lock()
        self._chunks = []
        self._echo = echo
        self._log_enabled = (DEFAULT_LOG_ENABLED
                             if log_enabled is None else log_enabled)
        self._log = None
        self._log_size = 0
        self.last_ts = 0.0
        self.first_ts = 0.0
        self.frame_count = 0
        self._terminal_ws = set()
        self.ws_open_count = 0
        self.ws_close_count = 0
        self.last_ws_open = 0.0
        self.last_ws_close = 0.0
        self.last_ws_error = ""

    def set_logging(self, enabled: bool) -> None:
        """大文件传输期间可暂停日志，避免把 Base64 回显再写一遍磁盘。"""
        with self._lock:
            self._log_enabled = enabled
            if not enabled and self._log is not None:
                self._log.close()
                self._log = None

    def _rotate_log(self) -> None:
        """Rotate the append-only terminal log without touching session state."""
        if self._log is not None:
            self._log.close()
            self._log = None
        if LOG_BACKUP_COUNT <= 0:
            if os.path.exists(LOG_FILE):
                os.remove(LOG_FILE)
        else:
            oldest = f"{LOG_FILE}.{LOG_BACKUP_COUNT}"
            if os.path.exists(oldest):
                os.remove(oldest)
            for index in range(LOG_BACKUP_COUNT - 1, 0, -1):
                src = f"{LOG_FILE}.{index}"
                dst = f"{LOG_FILE}.{index + 1}"
                if os.path.exists(src):
                    os.replace(src, dst)
            if os.path.exists(LOG_FILE):
                os.replace(LOG_FILE, f"{LOG_FILE}.1")
        self._log_size = 0

    def _open_log(self) -> None:
        if self._log is not None:
            return
        self._log_size = (os.path.getsize(LOG_FILE)
                          if os.path.exists(LOG_FILE) else 0)
        if LOG_MAX_BYTES > 0 and self._log_size >= LOG_MAX_BYTES:
            self._rotate_log()
        self._log = open(LOG_FILE, "a", encoding="utf-8",
                         buffering=256 * 1024)

    @property
    def has_output(self) -> bool:
        with self._lock:
            return self.frame_count > 0

    @property
    def ws_seen(self) -> bool:
        with self._lock:
            return self.ws_open_count > 0

    @property
    def ws_connected(self) -> bool:
        with self._lock:
            return bool(self._terminal_ws)

    def ws_opened(self, key: str) -> None:
        with self._lock:
            if key not in self._terminal_ws:
                self._terminal_ws.add(key)
                self.ws_open_count += 1
                self.last_ws_open = time.monotonic()
                self.last_ws_error = ""

    def ws_closed(self, key: str, error: str = "") -> None:
        with self._lock:
            was_open = key in self._terminal_ws
            self._terminal_ws.discard(key)
            if was_open:
                self.ws_close_count += 1
                self.last_ws_close = time.monotonic()
            if error:
                self.last_ws_error = error

    def push(self, text: str) -> None:
        with self._lock:
            if self._echo:
                sys.stdout.write(text)
                sys.stdout.flush()
            self._chunks.append(text)
            self.last_ts = time.time()
            if not self.first_ts:
                self.first_ts = self.last_ts
            self.frame_count += 1
            if self._log_enabled:
                encoded_size = len(text.encode("utf-8", errors="replace"))
                self._open_log()
                if (LOG_MAX_BYTES > 0 and self._log_size > 0
                        and self._log_size + encoded_size > LOG_MAX_BYTES):
                    self._rotate_log()
                    self._open_log()
                self._log.write(text)
                self._log_size += encoded_size

    def take(self) -> str:
        with self._lock:
            out = "".join(self._chunks)
            self._chunks.clear()
            return out

    def close(self) -> None:
        with self._lock:
            if self._log is not None:
                self._log.close()
                self._log = None


class _IncrUtf8:
    """增量 UTF-8 解码: 二进制帧可能在多字节字符中间截断."""

    def __init__(self):
        self.buf = b""

    def feed(self, chunk: bytes) -> str:
        self.buf += chunk
        try:
            out = self.buf.decode("utf-8")
            self.buf = b""
            return out
        except UnicodeDecodeError as e:
            out = self.buf[:e.start].decode("utf-8", errors="replace")
            self.buf = self.buf[e.start:]
            return out


def _decode_koko_frame(payload, utf8: _IncrUtf8) -> str:
    """KoKo WS 帧 -> 终端输出文本.

    KoKo v4: 终端内容以二进制帧直接传输; JSON 文本帧是控制消息
    (CONNECT / TERMINAL_SESSION 等), 忽略.
    """
    try:
        if isinstance(payload, (bytes, bytearray)):
            return utf8.feed(bytes(payload))
        if isinstance(payload, str):
            try:
                msg = json.loads(payload)
            except ValueError:
                return payload
            if msg.get("type") == "TERMINAL_DATA":
                raw = base64.b64decode((msg.get("data") or ""))
                return utf8.feed(raw)
            return ""
    except Exception:
        pass
    return ""


# ---------- WebSocket 输出捕获 ----------
WS_URLS = []


def hook_page(page, sink: TermSink) -> None:
    """用 CDP 捕获页面上的终端 WS 下行帧 (比 Playwright websocket 事件可靠).

    终端 WS 位于 koko/connect iframe 中, 输出以二进制/文本帧混合传输.
    """
    try:
        # Luna may briefly show/close a JavaScript dialog while restoring a
        # session.  Register a handler before network activity so Playwright
        # does not race a detached dialog (which otherwise aborts the bridge).
        page.on("dialog", lambda dialog: dialog.dismiss())
        cdp = page.context.new_cdp_session(page)
        cdp.send("Network.enable")
    except Exception as e:
        print(f"[!] CDP 会话创建失败: {e}", flush=True)
        return
    ws_map = {}       # requestId -> ws url
    decoders = {}     # requestId -> 增量 UTF-8 解码器
    page_key = str(id(page))

    def sink_key(req):
        # CDP 的 requestId 只保证在单个 target 中唯一。
        return f"{page_key}:{req}"

    def on_created(params):
        req = params["requestId"]
        url = params["url"]
        ws_map[req] = url

    def on_handshake(params):
        req = params.get("requestId", "")
        if "terminal" in ws_map.get(req, ""):
            # webSocketCreated 只表示发起连接，握手成功后才算真正在线。
            sink.ws_opened(sink_key(req))

    def on_frame(params):
        req = params.get("requestId", "")
        url = ws_map.get(req, "")
        if "terminal" not in url:
            return
        if url not in WS_URLS:
            WS_URLS.append(url)
        resp = params.get("response", {})
        payload = resp.get("payloadData", "")
        if not payload:
            return
        if resp.get("opcode") == 2:  # 二进制帧: payloadData 是 base64
            try:
                payload = base64.b64decode(payload)
            except Exception:
                return
        dec = decoders.get(req)
        if dec is None:
            dec = _IncrUtf8()
            decoders[req] = dec
        text = _decode_koko_frame(payload, dec)
        if text:
            sink.push(text)

    def on_closed(params):
        req = params.get("requestId", "")
        url = ws_map.pop(req, "")
        decoders.pop(req, None)
        if "terminal" in url:
            sink.ws_closed(sink_key(req))

    def on_error(params):
        req = params.get("requestId", "")
        url = ws_map.get(req, "")
        if "terminal" in url:
            sink.ws_closed(
                sink_key(req), params.get("errorMessage", "WebSocket error")
            )

    def close_all(error: str):
        for req, url in list(ws_map.items()):
            if "terminal" in url:
                sink.ws_closed(sink_key(req), error)
        ws_map.clear()
        decoders.clear()

    def on_page_close():
        close_all("terminal page closed")

    def on_frame_navigated(frame):
        try:
            if frame == page.main_frame:
                # 页面 reload/navigation 可能不会补发 webSocketClosed。
                close_all("terminal page navigated")
        except Exception:
            pass

    cdp.on("Network.webSocketCreated", on_created)
    cdp.on("Network.webSocketHandshakeResponseReceived", on_handshake)
    cdp.on("Network.webSocketFrameReceived", on_frame)
    cdp.on("Network.webSocketClosed", on_closed)
    cdp.on("Network.webSocketFrameError", on_error)
    page.on("close", on_page_close)
    page.on("framenavigated", on_frame_navigated)


# ---------- 页面操作 ----------
def _find_terminal(ctx):
    """在任意页面(含 iframe)里找已就绪的终端, 返回 (page, frame)."""
    for pg in ctx.pages:
        for f in pg.frames:
            try:
                if f.locator(".xterm").count() > 0:
                    return pg, f
            except Exception:
                continue
    return None, None


def _handle_connect_dialog(pg) -> bool:
    """处理 Luna 的"连接"对话框: 勾选下次自动登录并点击连接.

    返回 True 表示点击了连接按钮.
    """
    # 勾选 下次自动登录 (让以后的会话自动连接, 不再弹对话框)
    try:
        lbl = pg.locator("text=下次自动登录").first
        if lbl.count() > 0 and lbl.is_visible():
            lbl.click()
            pg.wait_for_timeout(300)
    except Exception:
        pass
    # 点击 连接 按钮 (Luna 的按钮可能是 a/button/div, 用精确文本匹配)
    try:
        btn = pg.get_by_text("连接", exact=True).first
        if btn.count() > 0 and btn.is_visible():
            btn.click()
            print("[+] 已点击'连接'按钮", flush=True)
            return True
    except Exception:
        pass
    return False


def _any_page_url(ctx, pred) -> bool:
    for pg in ctx.pages:
        try:
            if pred(pg.url):
                return True
        except Exception:
            continue
    return False


def wait_terminal(ctx, ctrl_page, sink, first_phase_s: int, total_s: int = 900):
    """等待任意标签页出现终端, 返回该页面; 失败返回 None."""
    from playwright._impl._errors import TargetClosedError
    pending = []  # 新打开的标签页, 由 poll 循环统一挂载监听

    def on_page(pg):
        pending.append(pg)

    ctx.on("page", on_page)

    deadline = time.time() + total_s
    t0 = time.time()
    login_prompted = False
    forced_login = False
    state_saved = False
    while time.time() < deadline:
        while pending:
            hook_page(pending.pop(), sink)
        try:
            tp, tf = _find_terminal(ctx)
            # xterm DOM 会在 WebSocket 关闭后继续留在页面里。旧实现只看
            # DOM，因此会把已经断开的终端误判为可用。
            if tp is not None and sink.ws_connected:
                if not state_saved:
                    save_state(ctx)
                    state_saved = True
                if login_prompted:
                    print("[+] 检测到已完成登录, 终端已就绪")
                return tp, tf
            on_login = _any_page_url(
                ctx, lambda u: "/core/auth/login/" in u)
        except TargetClosedError:
            print("[!] 浏览器窗口被关闭, 脚本退出")
            return None, None
        if on_login:
            if not login_prompted:
                print("[!] 需要登录: 请在浏览器窗口里完成登录(含验证码)")
                login_prompted = True
        else:
            # 有页面离开了登录页 = 登录成功, 立即保存会话以防再丢失
            if login_prompted and not state_saved:
                save_state(ctx)
                state_saved = True
        # 仅在从未见过登录页时才主动跳转 (登录模式下用户刚登录成功时
        # 页面也会短暂离开登录页, 绝不能因此把页面拉回登录页)
        if not forced_login and not on_login and not login_prompted \
                and time.time() - t0 > first_phase_s:
            print("[!] 未检测到有效会话, 主动跳转登录页...")
            forced_login = True
            try:
                ctrl_page.goto(LOGIN_URL, wait_until="domcontentloaded",
                               timeout=30000)
                continue
            except TargetClosedError:
                print("[!] 浏览器窗口被关闭, 脚本退出")
                return None, None
            except Exception as e:
                print(f"[!] 跳转失败: {e}")
        if not on_login:
            # "连接"对话框兜底: 勾选下次自动登录并点击连接
            for pg in ctx.pages:
                try:
                    if "/core/auth/login/" in pg.url:
                        continue
                    if _handle_connect_dialog(pg):
                        break
                except Exception:
                    continue
        try:
            # 250ms 足够让 Playwright 派发事件，同时显著减少已就绪终端的等待。
            ctrl_page.wait_for_timeout(250)
        except TargetClosedError:
            print("[!] 浏览器窗口被关闭, 脚本退出")
            return None, None
    print("[x] 等待终端超时")
    return None, None


def _wait_native_reconnect(page, sink: TermSink,
                           timeout_s: float = 18.0) -> bool:
    """给 KoKo 自带的 5 次重连留出时间，同时持续派发 CDP 事件。"""
    if sink.ws_connected:
        return True
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            page.wait_for_timeout(250)
        except Exception:
            return False
        if sink.ws_connected:
            return True
    return False


def _recover_terminal(ctx, ctrl_page, sink: TermSink, current_page=None,
                      native_grace_s: float = 18.0,
                      total_s: int = 180):
    """先等待 KoKo 原生重连，失败后刷新 Luna 并重新定位终端。"""
    pump_page = current_page or ctrl_page
    if _wait_native_reconnect(pump_page, sink, native_grace_s):
        tp, tf = _find_terminal(ctx)
        if tp is not None:
            print("[+] 终端 WebSocket 已自动恢复", flush=True)
            return tp, tf

    detail = f" ({sink.last_ws_error})" if sink.last_ws_error else ""
    print(f"[!] 终端连接未自动恢复{detail}，正在刷新 Luna...", flush=True)
    target = current_page
    try:
        if target is None or target.is_closed():
            target = ctrl_page
        if target is None or target.is_closed():
            target = ctx.new_page()
            hook_page(target, sink)
            target.goto(URL, wait_until="domcontentloaded", timeout=60000)
        else:
            target.reload(wait_until="domcontentloaded", timeout=60000)
    except Exception as exc:
        print(f"[!] 刷新终端页失败: {exc}", flush=True)
        try:
            target = ctx.new_page()
            hook_page(target, sink)
            target.goto(URL, wait_until="domcontentloaded", timeout=60000)
        except Exception as retry_exc:
            print(f"[x] 重新打开 Luna 失败: {retry_exc}", flush=True)
            return None, None

    tp, tf = wait_terminal(
        ctx, target, sink, first_phase_s=30, total_s=total_s
    )
    if tp is not None:
        print("[+] 终端连接已恢复", flush=True)
    return tp, tf


def _wait_first_output(page, sink: TermSink, timeout_s: float = 180) -> bool:
    """等待终端输出开始流动 (KoKo 会话真正就绪), 防止会话未建立时就发命令.

    用 page.wait_for_timeout 而不是 time.sleep: 同步 API 中 time.sleep
    会阻塞事件分发, 导致 WS 帧延迟数分钟才送达.
    """
    # xterm 出现前 shell prompt 可能已经收到；旧逻辑会忽略它，
    # 并在每次命令前多等 30~180 秒。
    if sink.has_output:
        return True
    mark = time.time()
    last_report = 0
    while time.time() - mark < timeout_s:
        if sink.last_ts > mark:
            return True
        elapsed = time.time() - mark
        if int(elapsed) // 30 > last_report:
            last_report = int(elapsed) // 30
            print(f"[*] 等待终端会话建立... {int(elapsed)}s", flush=True)
        page.wait_for_timeout(250)
    return False


def send_cmd(frame, cmd: str) -> None:
    """把命令输入到 xterm (终端位于 koko/connect 的 iframe 中).

    xterm.js 监听 keydown, 必须用 keyboard.type 产生真实键盘事件;
    insert_text 只产生 input 事件, xterm 不会响应.
    """
    ta = frame.locator(".xterm-helper-textarea")
    if ta.count() > 0:
        ta.focus()
    else:
        frame.locator(".xterm").click()
    pg = frame.page
    pg.keyboard.type(cmd)
    pg.keyboard.press("Enter")


def send_data(frame, data: str, end_with_ctrl_d: bool = True) -> None:
    """把大量文本(如 base64 数据)送入终端.

    xterm.js 只响应 keydown 事件, insert_text / 合成 paste 均不生效,
    因此用 keyboard.type(delay=0) 快速逐字符注入 (约 1500 字符/秒),
    500KB 大约需要 5 分钟. 大文件请配合 --upload 分块使用.
    """
    ta = frame.locator(".xterm-helper-textarea")
    if ta.count() > 0:
        ta.focus()
    else:
        frame.locator(".xterm").click()
    frame.page.keyboard.type(data, delay=0)
    if end_with_ctrl_d:
        frame.page.keyboard.press("Control+d")


def send_paste(frame, data: str) -> None:
    """Inject a paste event into xterm's helper textarea.

    Keyboard events are intentionally slow for multi-megabyte payloads. The
    xterm helper textarea accepts a trusted-shaped paste event and forwards it
    through the same input channel, while avoiding per-character automation.
    """
    ta = frame.locator(".xterm-helper-textarea")
    ta.focus()
    ta.evaluate("""(el, text) => {
        const dt = new DataTransfer();
        dt.setData('text/plain', text);
        el.dispatchEvent(new ClipboardEvent('paste', {
            clipboardData: dt, bubbles: true, cancelable: true
        }));
    }""", data)


def _wait_for_marker(page, sink: TermSink, marker: str,
                     timeout_s: float) -> tuple[bool, str]:
    """等待终端输出标记，同时持续泵送 Playwright 事件。"""
    deadline = time.monotonic() + timeout_s
    chunks = []
    disconnected_since = None
    while time.monotonic() < deadline:
        page.wait_for_timeout(50)
        if sink.ws_seen and not sink.ws_connected:
            if disconnected_since is None:
                disconnected_since = time.monotonic()
            elif time.monotonic() - disconnected_since >= 20:
                # KoKo 的原生重连窗口已经耗尽，避免按大文件总超时空等。
                break
        else:
            disconnected_since = None
        chunk = sink.take()
        if chunk:
            chunks.append(chunk)
            text = "".join(chunks)
            if marker in text:
                return True, text
            # 标记很短；限制异常输出占用，不影响跨帧匹配。
            if len(text) > 256 * 1024:
                chunks = [text[-4096:]]
    chunks.append(sink.take())
    return False, "".join(chunks)


def save_state(ctx) -> None:
    ctx.storage_state(path=STATE_FILE)
    print(f"[+] 会话已保存: {STATE_FILE}")


def _first_url_and_phase():
    """根据会话文件是否存在, 决定初始页面与第一阶段等待时长."""
    if os.path.exists(STATE_FILE):
        return URL, 90      # 有会话: 直连目标页, 90 秒内未就绪则转登录页
    return LOGIN_URL, 0     # 无会话: 直接进登录页


# ---------- 模式实现 ----------
def mode_login() -> int:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        ctx = browser.new_context(ignore_https_errors=True,
                                  viewport={"width": 1280, "height": 800})
        sink = TermSink(echo=True)
        page = ctx.new_page()
        hook_page(page, sink)
        page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=60000)
        tp, _tf = wait_terminal(ctx, page, sink, first_phase_s=0, total_s=900)
        if tp is None:
            return 1
        save_state(ctx)
        print(f"[+] 终端页面: {tp.url}")
        print(f"[+] 已观测到的终端 WS: {WS_URLS}")
        browser.close()
    print("[+] LOGIN OK, 下次运行 -c / REPL 模式将直接复用会话")
    return 0


def mode_run(cmd: str, wait_s: float, headless: bool,
             command_timeout_s: float = 240) -> int:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        kw = {"ignore_https_errors": True,
              "viewport": {"width": 1280, "height": 800}}
        if os.path.exists(STATE_FILE):
            kw["storage_state"] = STATE_FILE
        ctx = browser.new_context(**kw)
        sink = TermSink(echo=False)
        page = ctx.new_page()
        hook_page(page, sink)
        first_url, first_phase = _first_url_and_phase()
        page.goto(first_url, wait_until="domcontentloaded", timeout=60000)
        tp, tf = wait_terminal(ctx, page, sink, first_phase_s=first_phase,
                               total_s=900)
        if tp is None:
            return 1
        # 等会话真正就绪(第一批输出到达)后再发命令
        if not _wait_first_output(tp, sink, 30):
            print("[!] 30 秒内终端无输出, 仍继续发送", flush=True)
        tp.wait_for_timeout(100)
        sink.take()
        send_cmd(tf, cmd)
        t0 = time.time()
        collected = ""
        break_reason = ""
        exit_code = 0
        while True:
            tp.wait_for_timeout(200)
            collected += sink.take()
            if sink.ws_seen and not sink.ws_connected:
                print("[!] 命令执行期间终端连接中断，等待原生重连...",
                      flush=True)
                if not _wait_native_reconnect(tp, sink):
                    # 命令可能已经在远端产生副作用，不能安全地自动重放。
                    break_reason = "disconnected"
                    exit_code = 1
                    break
                print("[+] 终端 WebSocket 已恢复，继续收集输出", flush=True)
            quiet = time.time() - sink.last_ts
            if quiet >= wait_s and collected:
                break_reason = "quiet"
                break
            if time.time() - t0 > command_timeout_s:
                break_reason = "timeout"
                break
        collected += sink.take()
        out_file = os.path.join(HERE, "last_output.txt")
        with open(out_file, "w", encoding="utf-8") as f:
            f.write(collected)
        print(f"[+] 输出已保存: {out_file} (break={break_reason}, "
              f"len={len(collected)}, ws={WS_URLS})", flush=True)
        if sink.frame_count:
            print(f"[+] 帧时间线: 首次 {sink.first_ts:.1f} "
                  f"末次 {sink.last_ts:.1f} 共 {sink.frame_count} 帧",
                  flush=True)
        sys.stdout.write(collected)
        sys.stdout.flush()
        save_state(ctx)
        sink.close()
        browser.close()
    return exit_code


def mode_repl(headless: bool) -> int:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        kw = {"ignore_https_errors": True,
              "viewport": {"width": 1280, "height": 800}}
        if os.path.exists(STATE_FILE):
            kw["storage_state"] = STATE_FILE
        ctx = browser.new_context(**kw)
        sink = TermSink(echo=True)
        page = ctx.new_page()
        hook_page(page, sink)
        first_url, first_phase = _first_url_and_phase()
        page.goto(first_url, wait_until="domcontentloaded", timeout=60000)
        tp, tf = wait_terminal(ctx, page, sink, first_phase_s=first_phase,
                               total_s=900)
        if tp is None:
            return 1
        if not _wait_first_output(tp, sink, 180):
            print("[!] 180 秒内终端无输出, 仍进入 REPL", flush=True)
        tp.wait_for_timeout(100)
        sink.take()
        save_state(ctx)
        print("\n[+] 已连接网页终端. 输入命令后回车执行, 输入 exit 退出.\n")

        # input() 会阻塞 Playwright 同步事件循环，导致 WS close/reconnect 和
        # 终端输出都积压到下一次键盘操作。只把控制台读取放到后台线程，
        # 浏览器操作仍全部留在主线程（Playwright 对象不是线程安全的）。
        command_queue = queue.Queue()

        def read_console():
            while True:
                try:
                    command_queue.put(("command", input(">> ").strip()))
                except EOFError:
                    command_queue.put(("stop", None))
                    return
                except KeyboardInterrupt:
                    command_queue.put(("stop", None))
                    return

        threading.Thread(target=read_console, daemon=True).start()
        exit_code = 0
        try:
            while True:
                # 持续泵送浏览器/CDP 事件，即使用户长时间不输入。
                tp.wait_for_timeout(250)
                if sink.ws_seen and not sink.ws_connected:
                    recovered_page, recovered_frame = _recover_terminal(
                        ctx, page, sink, current_page=tp
                    )
                    if recovered_page is None:
                        print("[x] 终端连接恢复失败", flush=True)
                        exit_code = 1
                        break
                    tp, tf = recovered_page, recovered_frame
                    sink.take()

                try:
                    kind, value = command_queue.get_nowait()
                except queue.Empty:
                    continue
                if kind == "stop":
                    print()
                    break
                cmd = value
                if cmd.lower() in ("exit", "quit"):
                    break
                if cmd:
                    send_cmd(tf, cmd)
        except KeyboardInterrupt:
            print()
        sink.close()
        browser.close()
    return exit_code


def _sha256_file(path: str, chunk_size: int = 4 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as src:
        while True:
            chunk = src.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _upload_receiver_script(ready_marker: str) -> str:
    """生成无第三方依赖的远端流式接收器。"""
    return f"""import base64, hashlib, os, re, sys
path = sys.argv[1]
wire_left = int(sys.argv[2])
encoded_expected = int(sys.argv[3])
raw_expected = int(sys.argv[4])
sha_expected = sys.argv[5]
digest = hashlib.sha256()
written = 0
b64_carry = b''
escape_carry = b''
encoded_seen = 0
paste_sequences = (b'\\x1b[200~', b'\\x1b[201~')
out = open(path, 'wb')
print('{ready_marker}', flush=True)
try:
    while wire_left:
        chunk = os.read(sys.stdin.fileno(), min(262144, wire_left))
        if not chunk:
            raise EOFError('terminal input ended during upload')
        wire_left -= len(chunk)
        data = escape_carry + chunk
        escape_carry = b''
        if wire_left:
            for n in range(1, 6):
                tail = data[-n:]
                if any(seq.startswith(tail) for seq in paste_sequences):
                    escape_carry = tail
                    data = data[:-n]
                    break
        for seq in paste_sequences:
            data = data.replace(seq, b'')
        if re.search(rb'[^A-Za-z0-9+/=]', data):
            raise ValueError('unexpected terminal control data')
        encoded_seen += len(data)
        data = b64_carry + data
        usable = (len(data) // 4) * 4
        b64_carry = data[usable:]
        if usable:
            raw = base64.b64decode(data[:usable], validate=True)
            out.write(raw)
            digest.update(raw)
            written += len(raw)
    data = escape_carry
    for seq in paste_sequences:
        data = data.replace(seq, b'')
    if data:
        if re.search(rb'[^A-Za-z0-9+/=]', data):
            raise ValueError('unexpected trailing terminal control data')
        encoded_seen += len(data)
        b64_carry += data
    if b64_carry:
        raw = base64.b64decode(b64_carry, validate=True)
        out.write(raw)
        digest.update(raw)
        written += len(raw)
finally:
    out.close()
actual = digest.hexdigest()
if encoded_seen != encoded_expected:
    raise RuntimeError(f'base64 size mismatch: {{encoded_seen}} != {{encoded_expected}}')
if written != raw_expected:
    raise RuntimeError(f'size mismatch: {{written}} != {{raw_expected}}')
if actual != sha_expected:
    raise RuntimeError(f'sha256 mismatch: {{actual}} != {{sha_expected}}')
"""


def _mode_upload_terminal(local_file: str, remote_path: str, headless: bool,
                          rate_mib_s: float = 16.0) -> int:
    """流式上传文件；内存占用固定，并在原子替换前校验 SHA-256。"""
    if not os.path.isfile(local_file):
        print(f"[x] 本地文件不存在: {local_file}")
        return 2
    if rate_mib_s < 0:
        print("[x] --upload-rate 不能为负数")
        return 2

    file_size = os.path.getsize(local_file)
    encoded_size = ((file_size + 2) // 3) * 4
    raw_chunk_size = 192 * 1024  # 3 的倍数，块间不会产生 Base64 padding
    chunk_count = ((file_size + raw_chunk_size - 1) // raw_chunk_size
                   if file_size else 0)
    # xterm 的 paste 事件会给每块包一对 ESC[200~/ESC[201~ 控制序列。
    wire_size = encoded_size + chunk_count * 12
    local_sha256 = _sha256_file(local_file)
    token = uuid.uuid4().hex[:12]
    temp_path = f"{remote_path}.luna-upload-{token}"
    ready_marker = f"__LUNA_UPLOAD_READY_{token}__"
    done_marker = f"__LUNA_UPLOAD_DONE_{token}__"
    receiver = _upload_receiver_script(ready_marker)
    receiver_b64 = base64.b64encode(receiver.encode("utf-8")).decode("ascii")
    done_marker_b64 = base64.b64encode(done_marker.encode("ascii")).decode("ascii")
    q_temp = shlex.quote(temp_path)
    q_remote = shlex.quote(remote_path)
    bootstrap = (
        "stty -echo -icanon min 1 time 0; "
        f"python3 -c \"import base64;exec(base64.b64decode('{receiver_b64}'))\" "
        f"{q_temp} {wire_size} {encoded_size} {file_size} {local_sha256}; "
        "rc=$?; stty sane; "
        f"if [ $rc -eq 0 ]; then mv -f -- {q_temp} {q_remote}; rc=$?; fi; "
        f"if [ $rc -ne 0 ]; then rm -f -- {q_temp}; fi; "
        f"printf '\\n%s:%s\\n' "
        f"\"$(printf %s {done_marker_b64} | base64 -d)\" \"$rc\""
    )

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        kw = {"ignore_https_errors": True,
              "viewport": {"width": 1280, "height": 800}}
        if os.path.exists(STATE_FILE):
            kw["storage_state"] = STATE_FILE
        ctx = browser.new_context(**kw)
        sink = TermSink(echo=False)
        page = ctx.new_page()
        hook_page(page, sink)
        first_url, first_phase = _first_url_and_phase()
        page.goto(first_url, wait_until="domcontentloaded", timeout=60000)
        tp, tf = wait_terminal(ctx, page, sink, first_phase_s=first_phase,
                               total_s=900)
        if tp is None:
            return 1
        if not _wait_first_output(tp, sink, 180):
            print("[!] 180 秒内终端无输出, 仍继续发送", flush=True)
        sink.take()
        send_cmd(tf, bootstrap)

        ready, startup_output = _wait_for_marker(
            tp, sink, ready_marker, timeout_s=30)
        if not ready:
            print("[x] 远端流式接收器未就绪；请确认远端有 python3 且目标目录可写")
            if startup_output:
                print(f"[debug] 终端输出: {startup_output[-1000:]!r}")
            sink.close()
            browser.close()
            return 1

        sent_raw = 0
        started = time.monotonic()
        last_progress = started
        try:
            with open(local_file, "rb") as src:
                while True:
                    raw = src.read(raw_chunk_size)
                    if not raw:
                        break
                    if not sink.ws_connected:
                        print("[!] 上传期间终端连接中断，等待恢复...",
                              flush=True)
                        if not _wait_native_reconnect(tp, sink):
                            raise ConnectionError(
                                "terminal WebSocket did not reconnect"
                            )
                        print("[+] 上传连接已恢复", flush=True)
                    encoded = base64.b64encode(raw).decode("ascii")
                    send_paste(tf, encoded)
                    sent_raw += len(raw)

                    # 限制浏览器 WebSocket 待发送队列，避免大文件把内存撑满。
                    if rate_mib_s:
                        target = sent_raw / (rate_mib_s * 1024 * 1024)
                        delay = target - (time.monotonic() - started)
                        if delay > 0:
                            tp.wait_for_timeout(min(delay * 1000, 250))
                    else:
                        # 即使不限速，也要让 Playwright 定期派发网络事件。
                        tp.wait_for_timeout(1)

                    now = time.monotonic()
                    if now - last_progress >= 2 or sent_raw == file_size:
                        elapsed = max(now - started, 0.001)
                        speed = sent_raw / elapsed / (1024 * 1024)
                        percent = 100.0 if not file_size else sent_raw * 100 / file_size
                        print(f"[*] upload: {sent_raw}/{file_size} bytes "
                              f"({percent:.1f}%, {speed:.1f} MiB/s)",
                              flush=True)
                        last_progress = now
        except Exception as exc:
            print(f"[x] 上传发送失败: {exc}")
            try:
                tf.page.keyboard.press("Control+c")
                tp.wait_for_timeout(500)
            except Exception:
                pass
            sink.close()
            browser.close()
            return 1

        transfer_elapsed = max(time.monotonic() - started, 0.001)
        # send_paste 返回代表浏览器已入队，不代表 KoKo 已把数据写入 PTY。
        # 按保守的 0.5 MiB/s 等待远端排空，避免大文件被过早中断。
        finish_timeout = max(
            120.0, file_size / (512 * 1024) + 60.0
        )
        if done_marker in startup_output:
            done, finish_output = True, startup_output
        else:
            done, finish_output = _wait_for_marker(
                tp, sink, done_marker, timeout_s=finish_timeout)
        if not done:
            print("[x] 远端未确认完成，目标文件未被替换")
            try:
                tf.page.keyboard.press("Control+c")
                tp.wait_for_timeout(500)
            except Exception:
                pass
            sink.close()
            browser.close()
            return 1

        status = re.search(re.escape(done_marker) + r":(\d+)", finish_output)
        if status is None or status.group(1) != "0":
            print("[x] 远端校验或原子替换失败")
            print(f"[debug] 终端输出: {finish_output[-1500:]!r}")
            sink.close()
            browser.close()
            return 1

        speed = file_size / transfer_elapsed / (1024 * 1024)
        print(f"[+] upload 完成并通过 SHA-256 校验: "
              f"{local_file} -> {remote_path} "
              f"({file_size} bytes, {transfer_elapsed:.1f}s, "
              f"{speed:.1f} MiB/s)")
        save_state(ctx)
        sink.close()
        browser.close()
    return 0


def _open_elfinder_folder(page, label: str, timeout_ms: int = 60000) -> None:
    """在 elFinder 当前文件区打开指定目录，并等待 connector 响应。"""
    print(f"[*] WebSFTP 打开: {label}", flush=True)
    # elFinder 会在文件名里插入零宽空格，并且同一 title 也会出现在左侧
    # 目录树；限定 cwd 后按可见文本选最后一个，与实际文件卡片稳定匹配。
    item = page.locator(".elfinder-cwd-file").filter(
        has_text=label
    ).last
    item.wait_for(state="visible", timeout=timeout_ms)
    before_url = page.url
    with page.expect_response(
        lambda res: (
            "/koko/elfinder/connector/" in res.url
            and "cmd=open" in res.url
        ),
        timeout=timeout_ms,
    ) as response_info:
        item.dblclick()
    response = response_info.value
    if response.status != 200:
        raise RuntimeError(
            f"打开 SFTP 目录 {label!r} 失败: HTTP {response.status}"
        )
    page.wait_for_function(
        "(before) => window.location.href !== before",
        arg=before_url,
        timeout=timeout_ms,
    )
    page.wait_for_timeout(300)


def _mode_upload_sftp(local_file: str, remote_path: str,
                      headless: bool) -> int:
    """通过 JumpServer 原生 WebSFTP 上传，再由终端校验并移动到目标路径。"""
    if not os.path.isfile(local_file):
        print(f"[x] 本地文件不存在: {local_file}")
        return 2

    file_size = os.path.getsize(local_file)
    local_sha256 = _sha256_file(local_file)
    token = uuid.uuid4().hex[:12]
    stage_name = f"luna-upload-{token}-{os.path.basename(local_file)}"
    remote_stage = f"{SFTP_REMOTE_ROOT.rstrip('/')}/{stage_name}"

    # Playwright 的 path 型 file chooser 能真正流式读取磁盘；用同卷硬链接
    # 提供唯一文件名，避免覆盖 /tmp 中已有的同名文件，也不复制大文件。
    link_dir = tempfile.mkdtemp(
        prefix=".luna-upload-",
        dir=os.path.dirname(os.path.abspath(local_file)),
    )
    link_path = os.path.join(link_dir, stage_name)
    try:
        os.link(os.path.abspath(local_file), link_path)
    except Exception as exc:
        os.rmdir(link_dir)
        print(f"[!] 无法创建上传硬链接，将使用兼容通道: {exc}")
        return 1

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=headless)
            kw = {
                "ignore_https_errors": True,
                "viewport": {"width": 1280, "height": 800},
            }
            if os.path.exists(STATE_FILE):
                kw["storage_state"] = STATE_FILE
            ctx = browser.new_context(**kw)
            sink = TermSink(echo=False)
            page = ctx.new_page()
            hook_page(page, sink)
            first_url, first_phase = _first_url_and_phase()
            page.goto(first_url, wait_until="domcontentloaded",
                      timeout=60000)
            tp, tf = wait_terminal(
                ctx, page, sink, first_phase_s=first_phase, total_s=900
            )
            if tp is None:
                sink.close()
                browser.close()
                return 1
            if not _wait_first_output(tp, sink, 180):
                print("[!] 终端暂时无输出，继续建立 SFTP 通道", flush=True)

            fm = ctx.new_page()
            fm.goto(SFTP_URL, wait_until="domcontentloaded", timeout=60000)
            fm.locator(".elfinder-cwd-file").first.wait_for(
                state="visible", timeout=60000
            )
            for label in SFTP_NAV:
                _open_elfinder_folder(fm, label)

            print(f"[*] WebSFTP upload: {file_size} bytes", flush=True)
            upload_started = time.monotonic()
            fm.keyboard.press("Control+U")
            file_input = fm.locator(
                "input[type=file]:not([webkitdirectory])"
            )
            file_input.wait_for(state="attached", timeout=5000)
            file_input.set_input_files(link_path)

            # 等唯一暂存文件出现在目录中，意味着所有分片已合并且 connector
            # 已返回成功；大文件按保守的 0.5 MiB/s 计算超时。
            upload_timeout_ms = max(
                120000,
                int(file_size / (512 * 1024) * 1000 + 120000),
            )
            fm.locator(".elfinder-cwd-filename").filter(
                has_text=stage_name
            ).wait_for(state="visible", timeout=upload_timeout_ms)
            upload_elapsed = max(time.monotonic() - upload_started, 0.001)
            upload_speed = file_size / upload_elapsed / (1024 * 1024)
            print(f"[*] WebSFTP 已接收: {upload_elapsed:.1f}s, "
                  f"{upload_speed:.1f} MiB/s", flush=True)

            # WebSFTP 成功后再校验，并用 mv 替换最终目标；校验失败时目标
            # 文件保持不变，唯一暂存文件会被清理。
            if not sink.ws_connected:
                tp, tf = _recover_terminal(
                    ctx, page, sink, current_page=tp
                )
                if tp is None:
                    print("[x] 文件已传到暂存区，但终端断线且恢复失败；"
                          "未替换目标文件")
                    sink.close()
                    browser.close()
                    return 1
            verify_marker = f"__LUNA_SFTP_DONE_{token}__"
            verify_marker_b64 = base64.b64encode(
                verify_marker.encode("ascii")
            ).decode("ascii")
            q_stage = shlex.quote(remote_stage)
            q_remote = shlex.quote(remote_path)
            verify_cmd = (
                f"set -- $(sha256sum -- {q_stage}); actual=$1; "
                f"size=$(wc -c < {q_stage}); "
                f"if [ \"$actual\" = {local_sha256} ] "
                f"&& [ \"$size\" = {file_size} ]; then "
                f"mv -f -- {q_stage} {q_remote}; rc=$?; "
                f"else rc=91; rm -f -- {q_stage}; fi; "
                f"printf '\\n%s:%s\\n' "
                f"\"$(printf %s {verify_marker_b64} | base64 -d)\" \"$rc\""
            )
            sink.take()
            send_cmd(tf, verify_cmd)
            verified, verify_output = _wait_for_marker(
                tp, sink, verify_marker, timeout_s=300
            )
            status = (
                re.search(re.escape(verify_marker) + r":(\d+)", verify_output)
                if verified else None
            )
            if status is None or status.group(1) != "0":
                print("[x] WebSFTP 上传后的校验或移动失败")
                if verify_output:
                    print(f"[debug] 终端输出: {verify_output[-1500:]!r}")
                sink.close()
                browser.close()
                return 1

            total_elapsed = max(time.monotonic() - upload_started, 0.001)
            print(f"[+] upload 完成并通过 SHA-256 校验: "
                  f"{local_file} -> {remote_path} "
                  f"({file_size} bytes, {total_elapsed:.1f}s)")
            save_state(ctx)
            sink.close()
            browser.close()
        return 0
    except Exception as exc:
        print(f"[!] WebSFTP 上传不可用: {type(exc).__name__}: {exc}")
        return 1
    finally:
        try:
            os.unlink(link_path)
        finally:
            os.rmdir(link_dir)


def mode_upload(local_file: str, remote_path: str, headless: bool,
                rate_mib_s: float = 16.0, method: str = "auto") -> int:
    if method in ("auto", "sftp"):
        result = _mode_upload_sftp(local_file, remote_path, headless)
        if result in (0, 2) or method == "sftp":
            return result
        print("[*] 自动回退到终端流式上传", flush=True)
    return _mode_upload_terminal(
        local_file, remote_path, headless, rate_mib_s
    )


def _mode_download_sftp(remote_path: str, local_file: str,
                        headless: bool) -> int:
    """Stage a remote file in /tmp and stream it through WebSFTP."""
    local_file = os.path.abspath(local_file)
    local_dir = os.path.dirname(local_file)
    if not os.path.isdir(local_dir):
        print(f"[x] 本地目标目录不存在: {local_dir}")
        return 2

    token = uuid.uuid4().hex[:12]
    stage_name = f"luna-download-{token}-{os.path.basename(remote_path)}"
    remote_stage = f"{SFTP_REMOTE_ROOT.rstrip('/')}/{stage_name}"
    local_stage = f"{local_file}.luna-download-{token}"
    marker = f"__LUNA_DOWNLOAD_READY_{token}__"
    marker_b64 = base64.b64encode(marker.encode("ascii")).decode("ascii")
    q_remote = shlex.quote(remote_path)
    q_stage = shlex.quote(remote_stage)
    expiry_script = shlex.quote(f"sleep 3600; rm -f -- {remote_stage}")
    prepare_cmd = (
        f"rm -f -- {q_stage}; "
        f"if ln -- {q_remote} {q_stage} 2>/dev/null || "
        f"cp -- {q_remote} {q_stage}; then "
        f"set -- $(sha256sum -- {q_stage}); size=$(wc -c < {q_stage}); "
        f"printf '\\n%s:%s:%s\\n' "
        f"\"$(printf %s {marker_b64} | base64 -d)\" \"$size\" \"$1\"; "
        f"nohup sh -c {expiry_script} >/dev/null 2>&1 & "
        f"else printf '\\n%s:0:ERROR\\n' "
        f"\"$(printf %s {marker_b64} | base64 -d)\"; fi"
    )

    browser = None
    sink = None
    tp = None
    tf = None
    staged = False

    def cleanup_remote_stage() -> None:
        nonlocal staged
        if not staged or tp is None or tf is None or sink is None:
            return
        try:
            sink.take()
            send_cmd(tf, f"rm -f -- {q_stage}")
            tp.wait_for_timeout(300)
            sink.take()
            staged = False
        except Exception:
            pass

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=headless)
            kw = {
                "ignore_https_errors": True,
                "viewport": {"width": 1280, "height": 800},
                "accept_downloads": True,
            }
            if os.path.exists(STATE_FILE):
                kw["storage_state"] = STATE_FILE
            ctx = browser.new_context(**kw)
            sink = TermSink(echo=False)
            page = ctx.new_page()
            hook_page(page, sink)
            first_url, first_phase = _first_url_and_phase()
            page.goto(first_url, wait_until="domcontentloaded", timeout=60000)
            tp, tf = wait_terminal(
                ctx, page, sink, first_phase_s=first_phase, total_s=900
            )
            if tp is None:
                return 1
            if not _wait_first_output(tp, sink, 180):
                print("[!] 终端暂时无输出，继续建立下载通道", flush=True)

            sink.take()
            send_cmd(tf, prepare_cmd)
            ready, output = _wait_for_marker(tp, sink, marker, timeout_s=300)
            match = re.search(
                re.escape(marker) + r":(\d+):([0-9a-fA-F]{64})", output
            ) if ready else None
            if match is None:
                print("[x] 无法在远端暂存或校验下载文件")
                return 1
            expected_size = int(match.group(1))
            expected_sha256 = match.group(2).lower()
            staged = True

            fm = ctx.new_page()
            fm.goto(SFTP_URL, wait_until="domcontentloaded", timeout=60000)
            fm.locator(".elfinder-cwd-file").first.wait_for(
                state="visible", timeout=60000
            )
            for label in SFTP_NAV:
                _open_elfinder_folder(fm, label)

            filename = fm.locator(".elfinder-cwd-filename").filter(
                has_text=stage_name
            )
            filename.wait_for(state="visible", timeout=60000)
            item = filename.locator(
                "xpath=ancestor::*[contains(@class,'elfinder-cwd-file')][1]"
            )
            item.click()
            timeout_ms = max(
                120000,
                int(expected_size / (512 * 1024) * 1000 + 120000),
            )
            print(f"[*] WebSFTP download: {expected_size} bytes", flush=True)
            started = time.monotonic()
            with fm.expect_download(timeout=timeout_ms) as download_info:
                fm.locator(".elfinder-button-icon-download").first.click()
            download = download_info.value
            download.save_as(local_stage)

            actual_size = os.path.getsize(local_stage)
            actual_sha256 = _sha256_file(local_stage)
            if (actual_size != expected_size
                    or actual_sha256.lower() != expected_sha256):
                print("[x] WebSFTP 下载校验失败，未替换本地目标文件")
                cleanup_remote_stage()
                return 1
            os.replace(local_stage, local_file)
            elapsed = max(time.monotonic() - started, 0.001)
            speed = expected_size / elapsed / (1024 * 1024)
            print(f"[+] download 完成并通过 SHA-256 校验: "
                  f"{remote_path} -> {local_file} "
                  f"({expected_size} bytes, {elapsed:.1f}s, {speed:.1f} MiB/s)")
            cleanup_remote_stage()
            save_state(ctx)
            return 0
    except Exception as exc:
        print(f"[!] WebSFTP 下载不可用: {type(exc).__name__}: {exc}")
        return 1
    finally:
        if os.path.exists(local_stage):
            os.remove(local_stage)
        cleanup_remote_stage()
        if sink is not None:
            sink.close()
        if browser is not None:
            try:
                browser.close()
            except Exception:
                pass


def _mode_download_terminal(remote_path: str, local_file: str,
                            headless: bool) -> int:
    """从目标宿主机下载文件: 终端执行 base64 -w 200 打印内容
    (每行 200 字符, 避免 -w0 单行超长被终端滚动缓冲截断),
    捕获输出后正则提取 base64 解码写本地. (与 mode_upload 反向)"""
    import base64 as _b64
    import re
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        kw = {"ignore_https_errors": True,
              "viewport": {"width": 1280, "height": 800}}
        if os.path.exists(STATE_FILE):
            kw["storage_state"] = STATE_FILE
        ctx = browser.new_context(**kw)
        sink = TermSink(echo=False)
        page = ctx.new_page()
        hook_page(page, sink)
        first_url, first_phase = _first_url_and_phase()
        page.goto(first_url, wait_until="domcontentloaded", timeout=60000)
        tp, tf = wait_terminal(ctx, page, sink, first_phase_s=first_phase,
                               total_s=900)
        if tp is None:
            return 1
        if not _wait_first_output(tp, sink, 180):
            print("[!] 180 秒内终端无输出, 仍继续发送", flush=True)
        tp.wait_for_timeout(100)
        sink.take()
        q_remote = shlex.quote(remote_path)
        # 先查大小, 大文件加长静默等待, 防止流式输出间隙误判结束
        size = -1
        send_cmd(tf, f"wc -c < {q_remote}")
        t0 = time.time()
        size_output = ""
        while True:
            tp.wait_for_timeout(200)
            s = sink.take()
            size_output += s
            cleaned = re.sub(r"\x1b(?:\[[0-?]*[ -/]*[@-~]|\][^\x07]*(?:\x07|\x1b\\))", "", size_output)
            nums = [int(x) for x in re.findall(r"(?m)^\s*(\d+)\s*$", cleaned)]
            if nums:
                size = nums[-1]
                break
            if time.time() - t0 > 30:
                break
        tp.wait_for_timeout(500)
        sink.take()
        token = uuid.uuid4().hex[:12]
        begin_marker = f"__LUNA_B64_BEGIN_{token}__"
        end_marker = f"__LUNA_B64_END_{token}__"
        begin_b64 = base64.b64encode(begin_marker.encode()).decode("ascii")
        end_b64 = base64.b64encode(end_marker.encode()).decode("ascii")
        # 标记本身也用 Base64 构造，避免命令回显里的明文标记造成误判。
        cmd = (
            f"printf '%s\\n' \"$(printf %s {begin_b64} | base64 -d)\"; "
            f"base64 -w 200 -- {q_remote}; "
            f"printf '\\n%s\\n' \"$(printf %s {end_b64} | base64 -d)\""
        )
        # 大文件内容不落调试日志，避免重复磁盘 I/O 和日志无限膨胀。
        sink.set_logging(False)
        send_cmd(tf, cmd)
        t0 = time.time()
        collected_chunks = []
        marker_tail = ""
        quiet_limit = 10.0 if size > 200_000 else 3.0
        download_error = ""
        while True:
            tp.wait_for_timeout(200)
            chunk = sink.take()
            if chunk:
                collected_chunks.append(chunk)
                marker_tail = (marker_tail + chunk)[-512:]
                if end_marker in marker_tail:
                    break
            if sink.ws_seen and not sink.ws_connected:
                print("[!] 下载期间终端连接中断，等待原生重连...",
                      flush=True)
                if not _wait_native_reconnect(tp, sink):
                    download_error = "terminal disconnected"
                    break
            quiet = time.time() - sink.last_ts
            if quiet >= quiet_limit and collected_chunks:
                download_error = "output became quiet before end marker"
                break
            if time.time() - t0 > 600:
                download_error = "download timed out"
                break
        collected_chunks.append(sink.take())
        sink.set_logging(True)
        collected = "".join(collected_chunks)
        if download_error or end_marker not in collected:
            print(f"[x] 下载未完整结束: {download_error or 'end marker missing'}")
            sink.close()
            browser.close()
            return 1
        # 提取标记之间的 base64，避免长命令回显换行后混入 payload。
        body = collected.split("@")[0] if "@" in collected else collected
        body = body.replace("\r\n", "\n").replace("\r", "\n")
        body = re.sub(r"\x1b\[[0-9;?]*[A-Za-z]", "", body)
        if begin_marker in body:
            body = body.rsplit(begin_marker, 1)[1]
        if end_marker in body:
            body = body.split(end_marker, 1)[0]
        tokens = re.findall(r"[A-Za-z0-9+/=]{4,}", body)
        if not tokens:
            print("[!] 未捕获到 base64 内容")
            print(f"[debug] 回显片段: {collected[:300]!r}")
            return 1
        payload = "".join(tokens)
        try:
            raw = _b64.b64decode(payload, validate=True)
        except Exception as exc:
            print(f"[x] 下载内容 Base64 校验失败: {exc}")
            sink.close()
            browser.close()
            return 1
        with open(local_file, "wb") as f:
            f.write(raw)
        if size > 0 and len(raw) != size:
            print(f"[!] 大小不匹配: 远端 {size} bytes, 实际收到 {len(raw)} "
                  f"bytes —— 请改用分块下载")
            os.remove(local_file)
            save_state(ctx)
            sink.close()
            browser.close()
            return 1
        print(f"[+] download 完成: {remote_path} -> {local_file} "
              f"({len(raw)} bytes)")
        save_state(ctx)
        sink.close()
        browser.close()
    return 0


def mode_download(remote_path: str, local_file: str, headless: bool,
                  method: str = "auto") -> int:
    if method in ("auto", "sftp"):
        result = _mode_download_sftp(remote_path, local_file, headless)
        if result in (0, 2) or method == "sftp":
            return result
        print("[*] 自动回退到终端 Base64 下载", flush=True)
    return _mode_download_terminal(remote_path, local_file, headless)


def mode_sudo(command: str, password_file: str, headless: bool,
              wait_s: float = 3.0, command_timeout_s: float = 240) -> int:
    """Run one sudo command without placing the password in argv or logs."""
    with open(password_file, "r", encoding="utf-8") as f:
        secret = f.read().strip()
    if not secret:
        raise ValueError("password file is empty")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        kw = {"ignore_https_errors": True,
              "viewport": {"width": 1280, "height": 800}}
        if os.path.exists(STATE_FILE):
            kw["storage_state"] = STATE_FILE
        ctx = browser.new_context(**kw)
        sink = TermSink(echo=False)
        page = ctx.new_page()
        hook_page(page, sink)
        first_url, first_phase = _first_url_and_phase()
        page.goto(first_url, wait_until="domcontentloaded", timeout=60000)
        tp, tf = wait_terminal(ctx, page, sink, first_phase_s=first_phase,
                               total_s=900)
        if tp is None:
            return 1
        if not _wait_first_output(tp, sink, 30):
            print("[!] 30 秒内终端无输出, 仍继续发送", flush=True)
        tp.wait_for_timeout(100)
        sink.take()
        # Empty sudo prompt avoids putting any secret in the terminal output.
        send_cmd(tf, command)
        tp.wait_for_timeout(800)
        sink.take()
        send_cmd(tf, secret)
        t0 = time.time()
        collected = ""
        while True:
            tp.wait_for_timeout(200)
            collected += sink.take()
            quiet = time.time() - sink.last_ts
            if quiet >= wait_s and collected:
                break
            if time.time() - t0 > command_timeout_s:
                break
        collected += sink.take()
        # Never persist the password if a terminal implementation echoes it.
        safe = collected.replace(secret, "<REDACTED>")
        with open(os.path.join(HERE, "last_output.txt"), "w", encoding="utf-8") as f:
            f.write(safe)
        sys.stdout.write(safe)
        sys.stdout.flush()
        save_state(ctx)
        sink.close()
        browser.close()
    return 0


def main() -> int:
    global DEFAULT_LOG_ENABLED
    # argparse 的 --help 会在 parse_args 内直接退出，因此必须先设编码。
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace",
                               line_buffering=True)
    except Exception:
        pass
    ap = argparse.ArgumentParser(description="本地命令行 <-> Luna 网页终端交互桥")
    ap.add_argument("--login", action="store_true", help="手动登录并保存会话")
    ap.add_argument("-c", "--command", help="执行单条命令后退出")
    ap.add_argument("--headless", action="store_true", help="隐藏浏览器窗口")
    ap.add_argument("--wait", type=float, default=1.2,
                    help="-c 模式下等待输出静默的秒数")
    ap.add_argument("--command-timeout", type=float, default=240,
                    help="-c 模式的最长执行时间（秒，默认 240）")
    ap.add_argument("--upload", nargs=2, metavar=("LOCAL_FILE", "REMOTE_PATH"),
                    help="上传本地文件到目标宿主机指定路径")
    ap.add_argument("--upload-rate", type=float, default=16.0,
                    metavar="MIB_PER_SEC",
                    help="终端回退上传的发送速率上限（MiB/s，默认 16）")
    ap.add_argument("--upload-method",
                    choices=("auto", "sftp", "terminal"), default="auto",
                    help="上传通道（默认 auto：优先 WebSFTP，失败后回退）")
    ap.add_argument("--download", nargs=2, metavar=("REMOTE_PATH", "LOCAL_FILE"),
                    help="从目标宿主机下载文件到本地")
    ap.add_argument("--download-method",
                    choices=("auto", "sftp", "terminal"), default="auto",
                    help="下载通道（默认 auto：优先 WebSFTP，失败后回退）")
    ap.add_argument("--no-log", action="store_true",
                    help="本次运行不写 luna_term.log")
    ap.add_argument("--sudo-command", nargs=2, metavar=("COMMAND", "PASSWORD_FILE"),
                    help="安全执行 sudo 命令; 密码仅从本地文件读取并脱敏")
    args = ap.parse_args()
    DEFAULT_LOG_ENABLED = not args.no_log
    if not validate_config():
        return 2

    if args.login:
        return mode_login()
    if args.upload:
        return mode_upload(args.upload[0], args.upload[1], args.headless,
                           args.upload_rate, args.upload_method)
    if args.download:
        return mode_download(args.download[0], args.download[1], args.headless,
                             args.download_method)
    if args.sudo_command:
        return mode_sudo(args.sudo_command[0], args.sudo_command[1], args.headless,
                         args.wait, args.command_timeout)
    if args.command:
        return mode_run(args.command, args.wait, args.headless,
                        args.command_timeout)
    return mode_repl(args.headless)


if __name__ == "__main__":
    sys.exit(main())
