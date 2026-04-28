import os, re, logging, asyncio, zipfile, tempfile, shutil, time
from pathlib import Path
from collections import defaultdict
from PIL import Image
import img2pdf
from pyrogram import Client, filters
from pyrogram.errors import FloodWait

# ── CONFIG ─────────────────────────────────────────────────────────────────────
API_ID    = int(os.environ.get("API_ID"))
API_HASH  = os.environ.get("API_HASH")
BOT_TOKEN = os.environ.get("BOT_TOKEN")

SUPPORTED = {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".tiff", ".tif", ".gif"}
BATCH_WAIT = 4.0

logging.basicConfig(format="%(asctime)s | %(levelname)s | %(message)s", level=logging.INFO)
log = logging.getLogger(__name__)

app = Client(
    "cbz_bot_fresh",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    sleep_threshold=60,
    max_concurrent_transmissions=4,
    in_memory=True,
)

DOWNLOAD_SEM = asyncio.Semaphore(3)
user_queues:  dict[int, asyncio.Queue] = defaultdict(asyncio.Queue)
user_workers: dict[int, asyncio.Task]  = {}
STOP = object()

# ── PROGRESS ───────────────────────────────────────────────────────────────────
def bar(pct):
    return "█" * int(pct / 10) + "░" * (10 - int(pct / 10))

def make_text(step, pct, fname, extra=""):
    txt = f"**{fname}**\n\n{step}\n`{bar(pct)}` **{pct}%**"
    if extra:
        txt += f"\n\n__{extra}__"
    return txt

async def safe_edit(msg, text):
    try:
        await msg.edit_text(text)
    except FloodWait as e:
        await asyncio.sleep(e.value + 1)
        await safe_edit(msg, text)
    except Exception:
        pass

async def safe_delete(msg):
    try:
        await msg.delete()
    except Exception:
        pass

async def react(message):
    try:
        await message.react(emoji="⚡")
    except Exception:
        pass

def natural_key(name):
    return [int(c) if c.isdigit() else c.lower() for c in re.split(r'(\d+)', name)]

# ── EXTRACT ────────────────────────────────────────────────────────────────────
def extract_cbz(cbz_path, out_dir):
    if cbz_path.stat().st_size < 500:
        raise ValueError("__REDOWNLOAD__")
    if not zipfile.is_zipfile(cbz_path):
        raise ValueError("__REDOWNLOAD__")
    with zipfile.ZipFile(cbz_path, "r") as zf:
        safe_members = [n for n in zf.namelist() if ".." not in n and not n.startswith("/")]
        for member in safe_members:
            try:
                zf.extract(member, out_dir)
            except Exception:
                pass
    images = [p for p in out_dir.rglob("*") if p.is_file() and p.suffix.lower() in SUPPORTED]
    if not images:
        raise ValueError("No supported images found inside CBZ.")
    return sorted(images, key=lambda p: natural_key(p.name))

# ── CONVERT ────────────────────────────────────────────────────────────────────
def convert_to_pdf(images, pdf_path):
    safe, temps = [], []
    for img_path in images:
        try:
            with Image.open(img_path) as im:
                if im.mode in ("RGBA", "P", "LA", "L"):
                    conv = img_path.with_suffix("._c.jpg")
                    im.convert("RGB").save(conv, "JPEG", quality=95)
                    safe.append(conv)
                    temps.append(conv)
                else:
                    safe.append(img_path)
        except Exception as e:
            log.warning(f"Skipping {img_path.name}: {e}")
    if not safe:
        raise ValueError("All images unreadable.")
    try:
        with open(pdf_path, "wb") as f:
            f.write(img2pdf.convert([str(p) for p in safe]))
    except Exception:
        log.info("img2pdf failed — Pillow fallback")
        imgs = []
        for p in safe:
            try:
                imgs.append(Image.open(p).convert("RGB"))
            except Exception:
                pass
        if not imgs:
            raise ValueError("Pillow fallback also failed.")
        imgs[0].save(pdf_path, save_all=True, append_images=imgs[1:], format="PDF")
    finally:
        for p in temps:
            p.unlink(missing_ok=True)

# ── DOWNLOAD WITH RETRY ────────────────────────────────────────────────────────
async def do_download(message, cbz_path, status, fname):
    expected = message.document.file_size or 0
    min_size  = max(500, int(expected * 0.95))
    last_edit = [0.0]

    async def dl_progress(current, total):
        now = time.time()
        if now - last_edit[0] < 3.0:
            return
        last_edit[0] = now
        pct = max(5, int((current / total) * 30)) if total else 5
        await safe_edit(status, make_text(
            "📥 Downloading...", pct, fname,
            f"{current/1024/1024:.1f} / {total/1024/1024:.1f} MB"
        ))

    async with DOWNLOAD_SEM:
        for attempt in range(1, 9):
            try:
                if cbz_path.exists():
                    cbz_path.unlink()
                await app.download_media(message, file_name=str(cbz_path), progress=dl_progress)
                if cbz_path.exists() and cbz_path.stat().st_size >= min_size:
                    log.info(f"{fname}: download OK attempt {attempt}")
                    return
                log.warning(f"{fname}: attempt {attempt} size mismatch")
            except Exception as e:
                log.warning(f"{fname}: attempt {attempt} — {type(e).__name__}: {e}")

            wait = min(10 * attempt, 60)
            await safe_edit(status, make_text(f"⏳ Retrying ({attempt}/8)...", 5, fname, f"Waiting {wait}s"))
            await asyncio.sleep(wait)

    raise ValueError("Download failed after 8 attempts. Please resend.")

# ── SAFE SEND DOCUMENT ─────────────────────────────────────────────────────────
async def safe_send_document(**kwargs):
    for attempt in range(1, 6):
        try:
            return await app.send_document(**kwargs)
        except FloodWait as e:
            await asyncio.sleep(e.value + 1)
        except Exception as e:
            log.warning(f"send_document attempt {attempt} failed: {type(e).__name__}: {e}")
            if attempt == 5:
                raise
            await asyncio.sleep(3 * attempt)

# ── PROCESS ONE FILE ───────────────────────────────────────────────────────────
async def process_one(message):
    doc      = message.document
    fname    = doc.file_name or "file.cbz"
    stem     = Path(fname).stem
    pdf_name = f"{stem}.pdf"
    chat_id  = message.chat.id

    status = await app.send_message(chat_id, make_text("📥 Starting...", 0, fname))

    work_dir    = Path(tempfile.mkdtemp(prefix="cbzbot_"))
    cbz_path    = work_dir / fname
    extract_dir = work_dir / "extracted"
    extract_dir.mkdir()
    pdf_path    = work_dir / pdf_name

    try:
        # 1. DOWNLOAD
        await safe_edit(status, make_text("📥 Downloading...", 5, fname))
        await do_download(message, cbz_path, status, fname)
        await safe_edit(status, make_text("📥 Download complete!", 30, fname))

        # 2. EXTRACT
        await safe_edit(status, make_text("📂 Extracting...", 42, fname))
        loop = asyncio.get_event_loop()

        # Try extract up to 3 times — re-download if ZIP is invalid
        extract_ok = False
        for extract_attempt in range(1, 4):
            try:
                images = await loop.run_in_executor(None, extract_cbz, cbz_path, extract_dir)
                extract_ok = True
                break
            except ValueError as e:
                if "__REDOWNLOAD__" not in str(e):
                    raise
                log.warning(f"{fname}: ZIP invalid on extract attempt {extract_attempt}, re-downloading...")
                await safe_edit(status, make_text(
                    f"⏳ Re-downloading ({extract_attempt}/3)...", 10, fname,
                    "File incomplete — retrying"
                ))
                await asyncio.sleep(5 * extract_attempt)
                shutil.rmtree(extract_dir, ignore_errors=True)
                extract_dir.mkdir()
                await do_download(message, cbz_path, status, fname)

        if not extract_ok:
            raise ValueError("File could not be extracted after 3 attempts. Please resend.")

        page_count = len(images)
        await safe_edit(status, make_text("📂 Extracted!", 55, fname, f"{page_count} pages"))

        # 3. CONVERT
        await safe_edit(status, make_text("🖼️ Converting...", 70, fname, f"{page_count} pages → PDF"))
        await loop.run_in_executor(None, convert_to_pdf, images, pdf_path)
        pdf_mb = pdf_path.stat().st_size / 1024 / 1024
        await safe_edit(status, make_text("📄 PDF ready!", 88, fname, f"{pdf_mb:.1f} MB"))

        # 4. UPLOAD
        last_ul = [0.0]
        async def ul_progress(current, total):
            now = time.time()
            if now - last_ul[0] < 3.0:
                return
            last_ul[0] = now
            pct = 90 + int((current / total) * 9) if total else 92
            await safe_edit(status, make_text(
                "📤 Uploading...", pct, fname,
                f"{current/1024/1024:.1f} / {total/1024/1024:.1f} MB"
            ))

        await safe_send_document(
            chat_id=chat_id,
            document=str(pdf_path),
            file_name=pdf_name,
            caption=None,
            progress=ul_progress,
        )
        await safe_delete(status)

    except zipfile.BadZipFile:
        await safe_edit(status, f"❌ **{fname}**\n\nCorrupt ZIP. Please resend.")
    except ValueError as e:
        await safe_edit(status, f"❌ **{fname}**\n\n{e}")
    except Exception as e:
        log.exception(f"Unexpected error: {fname}")
        await safe_edit(status, f"❌ **{fname}**\n\nError: `{type(e).__name__}`")
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)
        log.info(f"Cleaned: {fname}")

# ── QUEUE WORKER ───────────────────────────────────────────────────────────────
async def queue_worker(chat_id):
    q = user_queues[chat_id]
    log.info(f"Worker started: chat {chat_id}")

    while True:
        msg = await q.get()
        if msg is STOP:
            q.task_done()
            break

        batch = [msg]
        deadline = time.time() + BATCH_WAIT
        while True:
            remaining = deadline - time.time()
            if remaining <= 0:
                break
            try:
                m = await asyncio.wait_for(q.get(), timeout=remaining)
                if m is STOP:
                    q.task_done()
                    break
                batch.append(m)
                deadline = time.time() + BATCH_WAIT
            except asyncio.TimeoutError:
                break

        batch.sort(key=lambda m: m.id)
        log.info(f"chat {chat_id}: {len(batch)} files → {[m.document.file_name for m in batch]}")

        for m in batch:
            try:
                await process_one(m)
            except Exception as e:
                log.exception(f"Worker error: {e}")
            finally:
                q.task_done()

    user_workers.pop(chat_id, None)
    log.info(f"Worker done: chat {chat_id}")

# ── HANDLERS ───────────────────────────────────────────────────────────────────
@app.on_message(filters.command("start"))
async def start_cmd(client, message):
    await react(message)
    await message.reply_text(
        "⚡ **CBZ → PDF Bot**\n\n"
        "Send me .cbz files — one or many.\n"
        "I'll convert each to PDF and send it back.\n\n"
        "✅ Files processed in **exact order** you send\n"
        "✅ Multiple users handled in **parallel**\n"
        "✅ Large files up to **2GB** supported\n"
        "✅ Smart retry — up to **8 attempts** per file\n\n"
        "Drop your CBZ files below ⬇️"
    )

@app.on_message(filters.document)
async def doc_handler(client, message):
    if message.sender_chat:
        return
    fname = (message.document.file_name or "").lower()
    await react(message)
    if not fname.endswith(".cbz"):
        await message.reply_text("⚠️ Please send `.cbz` files only.")
        return
    chat_id = message.chat.id
    await user_queues[chat_id].put(message)
    if chat_id not in user_workers or user_workers[chat_id].done():
        user_workers[chat_id] = asyncio.create_task(queue_worker(chat_id))

@app.on_message(filters.text & ~filters.command("start"))
async def text_handler(client, message):
    if message.forward_date or message.sender_chat:
        return
    await react(message)

# ── MAIN ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    log.info("CBZ→PDF Bot — HYBRID QUEUE (per-user FIFO + cross-user parallel)")
    app.run()