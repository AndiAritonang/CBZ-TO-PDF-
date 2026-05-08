import os, re, logging, asyncio, zipfile, tempfile, shutil, time
from pathlib import Path
from collections import defaultdict
from PIL import Image
import img2pdf
from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.errors import FloodWait

# ── CONFIG ─────────────────────────────────────────────────────────────────────
API_ID    = int(os.environ.get("API_ID",    "37623239"))
API_HASH  =     os.environ.get("API_HASH",  "9661c0bdbd8392709dd93139e8c3afcb")
BOT_TOKEN =     os.environ.get("BOT_TOKEN", "8663170411:AAGOMwGydm7c0Cq-7JedNAegbPdFIHq7-4c")

# All archive formats we accept — try to open as ZIP first (most CBR/CB7 are ZIP too)
ARCHIVE_EXTS = {".cbz", ".cbr", ".cb7", ".cbt", ".zip"}
SUPPORTED    = {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".tiff", ".tif", ".gif"}
BATCH_WAIT   = 4.0

logging.basicConfig(format="%(asctime)s | %(levelname)s | %(message)s", level=logging.INFO)
log = logging.getLogger(__name__)

app = Client(
    "cbz_session",
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

# ── ZIP OPEN CHECK ─────────────────────────────────────────────────────────────
def zip_is_openable(path):
    try:
        with zipfile.ZipFile(path, "r") as zf:
            _ = zf.namelist()
        return True
    except Exception:
        return False

# ── EXTRACT ────────────────────────────────────────────────────────────────────
def extract_archive(archive_path, out_dir):
    """
    Tries to open as ZIP (handles CBZ, CBR, CB7, ZIP — many are just renamed ZIPs).
    Extracts all supported images, sorts naturally.
    """
    try:
        with zipfile.ZipFile(archive_path, "r") as zf:
            for member in zf.namelist():
                if ".." in member or member.startswith("/"):
                    continue
                try:
                    zf.extract(member, out_dir)
                except Exception:
                    pass
    except zipfile.BadZipFile:
        raise ValueError("Could not open archive — unsupported format or corrupt file.")

    images = [
        p for p in out_dir.rglob("*")
        if p.is_file() and p.suffix.lower() in SUPPORTED
    ]
    if not images:
        raise ValueError("No supported images found inside the archive.")
    return sorted(images, key=lambda p: natural_key(p.name))

# ── CONVERT ────────────────────────────────────────────────────────────────────
def convert_to_pdf(images, pdf_path):
    """
    Memory-safe conversion: try img2pdf first (fast, lossless).
    Fall back to Pillow if img2pdf fails.
    Images processed one-by-one to avoid RAM spikes on large files.
    """
    safe, temps = [], []
    for img_path in images:
        try:
            with Image.open(img_path) as im:
                if im.mode in ("RGBA", "P", "LA", "L"):
                    conv = img_path.with_suffix("._c.jpg")
                    im.convert("RGB").save(conv, "JPEG", quality=90)
                    safe.append(conv)
                    temps.append(conv)
                else:
                    safe.append(img_path)
        except Exception as e:
            log.warning(f"Skipping {img_path.name}: {e}")

    if not safe:
        raise ValueError("All images were unreadable.")

    # Try img2pdf first
    try:
        with open(pdf_path, "wb") as f:
            f.write(img2pdf.convert([str(p) for p in safe]))
        return
    except Exception as e:
        log.info(f"img2pdf failed ({e}) — Pillow fallback")

    # Pillow fallback — open one by one, close immediately after appending
    try:
        pil_imgs = []
        for p in safe:
            try:
                im = Image.open(p).convert("RGB")
                pil_imgs.append(im)
            except Exception as ex:
                log.warning(f"Pillow skip {p.name}: {ex}")

        if not pil_imgs:
            raise ValueError("No readable images for Pillow fallback.")

        pil_imgs[0].save(
            pdf_path,
            save_all=True,
            append_images=pil_imgs[1:],
            format="PDF",
        )
    finally:
        for im in pil_imgs:
            try:
                im.close()
            except Exception:
                pass
        for p in temps:
            p.unlink(missing_ok=True)

# ── DOWNLOAD ───────────────────────────────────────────────────────────────────
async def do_download(message, archive_path, status, fname):
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

    waits = [5, 10, 20, 30, 45, 60, 60, 60]
    async with DOWNLOAD_SEM:
        for attempt in range(1, len(waits) + 2):
            try:
                if archive_path.exists():
                    archive_path.unlink()
                await app.download_media(message, file_name=str(archive_path), progress=dl_progress)
                actual = archive_path.stat().st_size if archive_path.exists() else 0
                if actual < min_size:
                    raise RuntimeError(f"Incomplete: {actual/1024/1024:.1f}MB / {expected/1024/1024:.1f}MB")
                if not zip_is_openable(archive_path):
                    raise RuntimeError("Archive not valid yet")
                log.info(f"{fname}: download OK attempt {attempt}")
                return
            except Exception as e:
                log.warning(f"{fname}: attempt {attempt} — {e}")
                if attempt > len(waits):
                    break
                wait = waits[attempt - 1]
                await safe_edit(status, make_text(
                    f"⏳ Retrying ({attempt}/{len(waits)+1})...",
                    5, fname, f"Waiting {wait}s"
                ))
                await asyncio.sleep(wait)

    raise ValueError("Download failed after all attempts. Please resend the file.")

# ── GET THUMBNAIL ──────────────────────────────────────────────────────────────
async def get_thumbnail(message, work_dir):
    """Download thumbnail from original message if present."""
    try:
        doc = message.document
        if doc.thumbs and len(doc.thumbs) > 0:
            thumb_path = work_dir / "thumb.jpg"
            await app.download_media(doc.thumbs[0].file_id, file_name=str(thumb_path))
            if thumb_path.exists() and thumb_path.stat().st_size > 0:
                return str(thumb_path)
    except Exception as e:
        log.warning(f"Thumbnail download failed: {e}")
    return None

# ── PROCESS ONE FILE ───────────────────────────────────────────────────────────
async def process_one(message):
    doc        = message.document
    orig_fname = doc.file_name or "file"
    stem       = os.path.splitext(orig_fname)[0]
    pdf_name   = stem + ".pdf"
    caption    = message.caption          # None if no caption — kept as-is
    chat_id    = message.chat.id

    status = await app.send_message(chat_id, make_text("📥 Starting...", 0, orig_fname))
    work_dir    = Path(tempfile.mkdtemp(prefix="cbzbot_"))
    archive_path = work_dir / orig_fname
    extract_dir  = work_dir / "extracted"
    extract_dir.mkdir()
    pdf_path     = work_dir / pdf_name

    try:
        # 1. DOWNLOAD
        await safe_edit(status, make_text("📥 Downloading...", 5, orig_fname))
        await do_download(message, archive_path, status, orig_fname)
        await safe_edit(status, make_text("📥 Done!", 30, orig_fname))

        # 2. THUMBNAIL (parallel with extract — don't block)
        thumb_task = asyncio.create_task(get_thumbnail(message, work_dir))

        # 3. EXTRACT
        await safe_edit(status, make_text("📂 Extracting...", 42, orig_fname))
        loop = asyncio.get_event_loop()
        images = await loop.run_in_executor(None, extract_archive, archive_path, extract_dir)
        page_count = len(images)
        await safe_edit(status, make_text("📂 Extracted!", 55, orig_fname, f"{page_count} pages"))

        # 4. CONVERT — with 10 min timeout to prevent silent hang
        await safe_edit(status, make_text("🖼️ Converting...", 70, orig_fname, f"{page_count} pages → PDF"))
        try:
            await asyncio.wait_for(
                loop.run_in_executor(None, convert_to_pdf, images, pdf_path),
                timeout=600  # 10 minutes max
            )
        except asyncio.TimeoutError:
            raise ValueError("Conversion timed out. File may be too large or complex.")
        pdf_mb = pdf_path.stat().st_size / 1024 / 1024
        await safe_edit(status, make_text("📄 PDF ready!", 88, orig_fname, f"{pdf_mb:.1f} MB"))

        # 5. UPLOAD — with original thumbnail + original caption
        thumb_path = await thumb_task  # get thumbnail result

        last_ul = [0.0]
        async def ul_progress(current, total):
            now = time.time()
            if now - last_ul[0] < 3.0:
                return
            last_ul[0] = now
            pct = 90 + int((current / total) * 9) if total else 92
            await safe_edit(status, make_text(
                "📤 Uploading...", pct, orig_fname,
                f"{current/1024/1024:.1f} / {total/1024/1024:.1f} MB"
            ))

        await app.send_document(
            chat_id=chat_id,
            document=str(pdf_path),
            file_name=pdf_name,
            caption=caption,          # original caption or None
            thumb=thumb_path,         # original thumbnail or None
            progress=ul_progress,
        )
        await safe_delete(status)

    except zipfile.BadZipFile:
        await safe_edit(status, f"❌ **{orig_fname}**\n\nCorrupt or unsupported archive.")
    except ValueError as e:
        await safe_edit(status, f"❌ **{orig_fname}**\n\n{e}")
    except Exception as e:
        log.exception(f"Unexpected error: {orig_fname}")
        await safe_edit(status, f"❌ **{orig_fname}**\n\nError: `{type(e).__name__}`")
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)
        log.info(f"Cleaned: {orig_fname}")

# ── QUEUE WORKER ───────────────────────────────────────────────────────────────
async def queue_worker(chat_id):
    q = user_queues[chat_id]
    log.info(f"Worker started: chat {chat_id}")
    while True:
        first = await q.get()
        batch = [first]
        deadline = time.time() + BATCH_WAIT
        while True:
            remaining = deadline - time.time()
            if remaining <= 0:
                break
            try:
                msg = await asyncio.wait_for(q.get(), timeout=remaining)
                batch.append(msg)
                deadline = time.time() + BATCH_WAIT
            except asyncio.TimeoutError:
                break
        batch.sort(key=lambda m: m.id)
        log.info(f"chat {chat_id}: {len(batch)} files → {[m.document.file_name for m in batch]}")
        for msg in batch:
            try:
                await process_one(msg)
            except Exception as e:
                log.exception(f"Worker error: {e}")
            finally:
                q.task_done()
        if q.empty():
            break
    user_workers.pop(chat_id, None)
    log.info(f"Worker done: chat {chat_id}")

# ── HANDLERS ───────────────────────────────────────────────────────────────────
@app.on_message(filters.command("start"))
async def start_cmd(client, message):
    await react(message)
    await message.reply_text(
        "Iam PArshyas CBZ TO PDF bot...!!!\n\n"
        "⚡ **CBZ → PDF Bot**\n\n"
        "Send me .cbz files — one or many....!!!\n"
        "I'll convert each to PDF and send it back...!!!\n\n"
        "✅ Files processed in **exact order** you send\n"
        "✅ Multiple users handled in **parallel**\n"
        "✅ Large files up to **2GB** supported\n"
        "✅ Smart retry for slow uploads\n\n"
        "Drop your CBZ files below ⬇️"
    )

@app.on_message(filters.document)
async def doc_handler(client, message):
    if message.sender_chat:
        return
    fname = (message.document.file_name or "").lower()
    ext   = os.path.splitext(fname)[1]
    await react(message)
    if ext not in ARCHIVE_EXTS:
        await message.reply_text(
            f"⚠️ Unsupported format `{ext}`\n"
            f"Supported: `{', '.join(sorted(ARCHIVE_EXTS))}`"
        )
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

if __name__ == "__main__":
    log.info("CBZ→PDF Bot starting...")
    app.run()