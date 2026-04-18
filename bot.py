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
BOT_TOKEN =     os.environ.get("BOT_TOKEN", "8663170411:AAEer7ziKHmqIg1TZ-7QN_jzSd17aH6gNfc")
SUPPORTED = {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".tiff", ".tif", ".gif"}

logging.basicConfig(format="%(asctime)s | %(levelname)s | %(message)s", level=logging.INFO)
log = logging.getLogger(__name__)

app = Client(
    "cbz_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    sleep_threshold=60,
    max_concurrent_transmissions=4,
)

DOWNLOAD_SEM = asyncio.Semaphore(3)

user_queues:  dict[int, asyncio.Queue] = defaultdict(asyncio.Queue)
user_workers: dict[int, asyncio.Task]  = {}

# ── PROGRESS HELPERS ───────────────────────────────────────────────────────────
def bar(pct: int) -> str:
    filled = int(pct / 10)
    return "█" * filled + "░" * (10 - filled)

def make_text(step: str, pct: int, fname: str, extra: str = "") -> str:
    txt = f"**{fname}**\n\n{step}\n`{bar(pct)}` **{pct}%**"
    if extra:
        txt += f"\n\n__{extra}__"
    return txt

async def safe_edit(msg: Message, text: str) -> None:
    try:
        await msg.edit_text(text)
    except FloodWait as e:
        await asyncio.sleep(e.value + 1)
        await safe_edit(msg, text)
    except Exception:
        pass

async def react(message: Message) -> None:
    try:
        await message.react(emoji="⚡")
    except Exception:
        pass

# ── NATURAL SORT ───────────────────────────────────────────────────────────────
def natural_key(name: str) -> list:
    return [int(c) if c.isdigit() else c.lower() for c in re.split(r'(\d+)', name)]

# ── EXTRACT CBZ ────────────────────────────────────────────────────────────────
def extract_cbz(cbz_path: Path, out_dir: Path) -> list:
    size = cbz_path.stat().st_size
    if size < 500:
        raise ValueError("__REDOWNLOAD__")
    if not zipfile.is_zipfile(cbz_path):
        raise ValueError("__REDOWNLOAD__")
    with zipfile.ZipFile(cbz_path, "r") as zf:
        safe_members = [n for n in zf.namelist() if ".." not in n and not n.startswith("/")]
        if not safe_members:
            raise ValueError("No safe files found in archive.")
        for member in safe_members:
            try:
                zf.extract(member, out_dir)
            except Exception:
                pass
    images = [
        p for p in out_dir.rglob("*")
        if p.is_file() and p.suffix.lower() in SUPPORTED
    ]
    if not images:
        raise ValueError("No supported images found inside CBZ.")
    return sorted(images, key=lambda p: natural_key(p.name))

# ── CONVERT TO PDF ─────────────────────────────────────────────────────────────
def convert_to_pdf(images: list, pdf_path: Path) -> None:
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
        raise ValueError("All images were unreadable.")

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
async def do_download(
    message: Message,
    cbz_path: Path,
    status: Message,
    fname: str,
) -> None:
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
                await app.download_media(
                    message,
                    file_name=str(cbz_path),
                    progress=dl_progress,
                )
                if cbz_path.exists() and cbz_path.stat().st_size >= min_size:
                    log.info(f"{fname}: downloaded OK on attempt {attempt}")
                    return
                log.warning(f"{fname}: attempt {attempt} size mismatch")
            except Exception as e:
                log.warning(f"{fname}: attempt {attempt} error: {type(e).__name__}: {e}")

            wait = min(10 * attempt, 60)
            await safe_edit(status, make_text(
                f"⏳ Retrying download ({attempt}/8)...",
                5, fname, f"Waiting {wait}s"
            ))
            await asyncio.sleep(wait)

    raise ValueError("Download failed after 8 attempts. Please resend the file.")

# ── PROCESS ONE FILE ───────────────────────────────────────────────────────────
async def process_one(message: Message) -> None:
    doc      = message.document
    fname    = doc.file_name or "file.cbz"
    stem     = Path(fname).stem
    pdf_name = f"{stem}.pdf"
    chat_id  = message.chat.id

    status = await app.send_message(
        chat_id,
        make_text("📥 Starting...", 0, fname)
    )

    work_dir    = Path(tempfile.mkdtemp(prefix="cbzbot_"))
    cbz_path    = work_dir / fname
    extract_dir = work_dir / "extracted"
    extract_dir.mkdir()
    pdf_path    = work_dir / pdf_name

    try:
        await safe_edit(status, make_text("📥 Downloading...", 5, fname))
        await do_download(message, cbz_path, status, fname)
        await safe_edit(status, make_text("📥 Download complete!", 30, fname))

        await safe_edit(status, make_text("📂 Extracting...", 42, fname))
        loop = asyncio.get_event_loop()
        try:
            images = await loop.run_in_executor(None, extract_cbz, cbz_path, extract_dir)
        except ValueError as e:
            if "__REDOWNLOAD__" in str(e):
                log.warning(f"{fname}: ZIP invalid — re-downloading")
                await safe_edit(status, make_text("⏳ Re-downloading (corrupt)...", 5, fname))
                shutil.rmtree(extract_dir, ignore_errors=True)
                extract_dir.mkdir()
                await do_download(message, cbz_path, status, fname)
                images = await loop.run_in_executor(None, extract_cbz, cbz_path, extract_dir)
            else:
                raise

        page_count = len(images)
        await safe_edit(status, make_text("📂 Extracted!", 55, fname, f"{page_count} pages"))

        await safe_edit(status, make_text("🖼️ Converting...", 70, fname, f"{page_count} pages → PDF"))
        await loop.run_in_executor(None, convert_to_pdf, images, pdf_path)
        pdf_mb = pdf_path.stat().st_size / 1024 / 1024
        await safe_edit(status, make_text("📄 PDF ready!", 88, fname, f"{pdf_mb:.1f} MB"))

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

        await app.send_document(
            chat_id=chat_id,
            document=str(pdf_path),
            file_name=pdf_name,
            caption=None,
            progress=ul_progress,
        )
        try:
            await status.delete()
        except Exception:
            pass

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

# ── PER-USER QUEUE WORKER ──────────────────────────────────────────────────────
async def queue_worker(chat_id: int) -> None:
    q = user_queues[chat_id]
    log.info(f"Worker started for chat {chat_id}")
    while True:
        message = await q.get()
        try:
            await process_one(message)
        except Exception as e:
            log.exception(f"Worker caught unhandled error for chat {chat_id}: {e}")
        finally:
            q.task_done()
        if q.empty():
            break
    user_workers.pop(chat_id, None)
    log.info(f"Worker finished for chat {chat_id}")

# ── HANDLERS ───────────────────────────────────────────────────────────────────
@app.on_message(filters.command("start"))
async def start_cmd(client: Client, message: Message) -> None:
    await react(message)
    await message.reply_text(
        "⚡ **CBZ → PDF Bot**\n\n"
        "Send me .cbz files — one or many....!!!\n"
        "I'll convert each to PDF and send it back...!!!\n\n"
        "✅ Files processed in **exact order**\n"
        "✅ Multiple users handled in **parallel**\n"
        "✅ Large files up to **2GB** supported\n"
        "✅ Smart retry — up to **8 attempts**\n\n"
        "Drop your CBZ files below ⬇️"
    )

@app.on_message(filters.document)
async def doc_handler(client: Client, message: Message) -> None:
    fname = (message.document.file_name or "").lower()
    await react(message)

    if not fname.endswith(".cbz"):
        await message.reply_text("⚠️ Please send `.cbz` files only.")
        return

    chat_id = message.chat.id

    await user_queues[chat_id].put(message)

    if chat_id not in user_workers or user_workers[chat_id].done():
        user_workers[chat_id] = asyncio.create_task(queue_worker(chat_id))

@app.on_message(filters.text & \~filters.command("start"))
async def text_handler(client: Client, message: Message) -> None:
    if message.forward_date or message.sender_chat:
        return
    await react(message)

# ── MAIN ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    log.info("CBZ→PDF Bot Started — Clean & Fixed Version")
    app.run() 