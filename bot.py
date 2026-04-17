import os, logging, asyncio, zipfile, tempfile, shutil
from pathlib import Path
from collections import defaultdict
from natsort import natsorted
from PIL import Image
import img2pdf
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes, filters
from telegram.constants import ParseMode
from telegram.error import TimedOut, NetworkError, RetryAfter

# ─── CONFIG ───────────────────────────────────────────────────────────────────
BOT_TOKEN = os.environ.get("BOT_TOKEN", "8663170411:AAEer7ziKHmqIg1TZ-7QN_jzSd17aH6gNfc")
MAX_MB    = int(os.environ.get("MAX_FILE_SIZE_MB", 125))
MAX_BYTES = MAX_MB * 1024 * 1024
SUPPORTED = {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".tiff", ".tif", ".gif"}

logging.basicConfig(format="%(asctime)s | %(levelname)s | %(message)s", level=logging.INFO)
log = logging.getLogger(__name__)

# ─── PER-USER QUEUE ───────────────────────────────────────────────────────────
user_queues:  dict[int, asyncio.Queue] = defaultdict(asyncio.Queue)
user_workers: dict[int, asyncio.Task]  = {}

# ─── PROGRESS HELPER ──────────────────────────────────────────────────────────
async def prog(msg, text: str):
    try:
        await msg.edit_text(text, parse_mode=ParseMode.MARKDOWN)
    except RetryAfter as e:
        await asyncio.sleep(e.retry_after + 1)
        await prog(msg, text)
    except Exception:
        pass

def bar(pct: int) -> str:
    filled = int(pct / 10)
    return "█" * filled + "░" * (10 - filled)

def status_text(step: str, pct: int, filename: str, extra: str = "") -> str:
    return (
        f"*{filename}*\n\n"
        f"{step}\n"
        f"`{bar(pct)}` *{pct}%*"
        + (f"\n\n_{extra}_" if extra else "")
    )

# ─── CONVERSION LOGIC ─────────────────────────────────────────────────────────
def extract_cbz(cbz_path: Path, out_dir: Path) -> list[Path]:
    if not zipfile.is_zipfile(cbz_path):
        raise ValueError("Not a valid CBZ/ZIP file.")
    with zipfile.ZipFile(cbz_path, "r") as zf:
        for name in zf.namelist():
            if ".." in name or name.startswith("/"):
                raise ValueError("Unsafe path in archive.")
        zf.extractall(out_dir)
    images = [p for p in out_dir.rglob("*") if p.is_file() and p.suffix.lower() in SUPPORTED]
    if not images:
        raise ValueError("No images found inside the CBZ file.")
    return natsorted(images, key=lambda p: p.name.lower())

def convert_to_pdf(images: list[Path], pdf_path: Path) -> None:
    safe, temps = [], []
    for img_path in images:
        try:
            with Image.open(img_path) as im:
                if im.mode in ("RGBA", "P", "LA", "L"):
                    conv = img_path.with_suffix("._conv.jpg")
                    im.convert("RGB").save(conv, "JPEG", quality=95)
                    safe.append(conv); temps.append(conv)
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
        log.info("img2pdf failed → Pillow fallback")
        pil_imgs = []
        for p in safe:
            try: pil_imgs.append(Image.open(p).convert("RGB"))
            except Exception: pass
        if not pil_imgs:
            raise ValueError("Could not read images with Pillow either.")
        pil_imgs[0].save(pdf_path, save_all=True, append_images=pil_imgs[1:], format="PDF")
    finally:
        for p in temps: p.unlink(missing_ok=True)

# ─── PROCESS ONE FILE ─────────────────────────────────────────────────────────
async def process_one(chat_id: int, document, context: ContextTypes.DEFAULT_TYPE):
    fname    = document.file_name or "file.cbz"
    stem     = Path(fname).stem
    pdf_name = f"{stem}.pdf"

    # Size check
    if (document.file_size or 0) > MAX_BYTES:
        sz = (document.file_size or 0) / 1024 / 1024
        await context.bot.send_message(
            chat_id,
            f"❌ *{fname}* is {sz:.1f} MB — max allowed is {MAX_MB} MB.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    # Initial status message
    status = await context.bot.send_message(
        chat_id,
        status_text("📥 Receiving file...", 0, fname),
        parse_mode=ParseMode.MARKDOWN,
    )

    work_dir    = Path(tempfile.mkdtemp(prefix="cbzbot_"))
    cbz_path    = work_dir / fname
    extract_dir = work_dir / "extracted"
    extract_dir.mkdir()
    pdf_path    = work_dir / pdf_name

    try:
        # ── 1. DOWNLOAD (0 → 10%) ───────────────────────────────────────────
        await prog(status, status_text("📥 Downloading file...", 5, fname))
        tg_file = await document.get_file()
        await tg_file.download_to_drive(cbz_path)
        await prog(status, status_text("📥 File received!", 10, fname))

        # ── 2. EXTRACT (10 → 40%) ───────────────────────────────────────────
        await prog(status, status_text("📂 Extracting CBZ...", 15, fname))
        loop = asyncio.get_event_loop()
        images = await loop.run_in_executor(None, extract_cbz, cbz_path, extract_dir)
        page_count = len(images)
        await prog(status, status_text("📂 Extraction done!", 40, fname, f"{page_count} pages found"))

        # ── 3. PROCESS IMAGES (40 → 70%) ────────────────────────────────────
        await prog(status, status_text("🖼️ Processing images...", 55, fname, f"Optimizing {page_count} pages"))
        await asyncio.sleep(0.5)  # slight pause so user sees this step
        await prog(status, status_text("🖼️ Images ready!", 70, fname))

        # ── 4. CREATE PDF (70 → 90%) ─────────────────────────────────────────
        await prog(status, status_text("📄 Creating PDF...", 75, fname))
        await loop.run_in_executor(None, convert_to_pdf, images, pdf_path)
        pdf_mb = pdf_path.stat().st_size / 1024 / 1024
        await prog(status, status_text("📄 PDF created!", 90, fname, f"Size: {pdf_mb:.1f} MB"))

        # ── 5. UPLOAD (90 → 100%) ────────────────────────────────────────────
        await prog(status, status_text("📤 Uploading PDF...", 95, fname))
        with open(pdf_path, "rb") as f:
            await context.bot.send_document(
                chat_id=chat_id,
                document=f,
                filename=pdf_name,
                caption=(
                    f"✅ *{pdf_name}*\n"
                    f"📄 Pages: *{page_count}*\n"
                    f"📦 Size: *{pdf_mb:.1f} MB*"
                ),
                parse_mode=ParseMode.MARKDOWN,
            )
        await prog(status, status_text("✅ Done!", 100, fname, "PDF sent successfully"))

    except zipfile.BadZipFile:
        await prog(status, f"❌ *{fname}*\n\nCorrupt or invalid CBZ file.")
    except ValueError as e:
        await prog(status, f"❌ *{fname}*\n\n{e}")
    except (TimedOut, NetworkError):
        await prog(status, f"❌ *{fname}*\n\nNetwork error. Please try again.")
    except Exception as e:
        log.exception(f"Error processing {fname}")
        await prog(status, f"❌ *{fname}*\n\nUnexpected error: `{type(e).__name__}`")
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)
        log.info(f"Cleaned up: {fname}")

# ─── QUEUE WORKER ─────────────────────────────────────────────────────────────
async def worker(chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    q = user_queues[chat_id]
    while True:
        document = await q.get()
        await process_one(chat_id, document, context)
        q.task_done()
        if q.empty():
            user_workers.pop(chat_id, None)
            break

# ─── HANDLERS ─────────────────────────────────────────────────────────────────
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 *CBZ → PDF Bot*\n\n"
        "📦 Send me one or multiple `.cbz` files.\n"
        "I'll convert each one to PDF and send it back.\n\n"
        "*Features:*\n"
        "• ✅ Multiple files → processed one by one\n"
        "• ✅ Real-time progress updates\n"
        f"• ✅ Up to {MAX_MB}MB per file\n"
        "• ✅ Auto cleanup after conversion\n\n"
        "Just drop your CBZ files below ⬇️",
        parse_mode=ParseMode.MARKDOWN,
    )

async def doc_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    doc = update.message.document
    if not doc:
        return

    fname = (doc.file_name or "").lower()
    if not fname.endswith(".cbz"):
        await update.message.reply_text(
            "⚠️ Please send `.cbz` files only.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    chat_id = update.effective_chat.id
    q = user_queues[chat_id]
    await q.put(doc)

    # Tell user it's queued (only if already processing something)
    pos = q.qsize()
    if pos > 1:
        await update.message.reply_text(
            f"✅ *{doc.file_name}* added to queue. Position: *#{pos}*",
            parse_mode=ParseMode.MARKDOWN,
        )

    # Spawn worker if not running
    if chat_id not in user_workers or user_workers[chat_id].done():
        task = asyncio.create_task(worker(chat_id, context))
        user_workers[chat_id] = task

async def other_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📎 Please send a `.cbz` file to convert.\nUse /start for help.",
        parse_mode=ParseMode.MARKDOWN,
    )

# ─── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    log.info("Starting CBZ → PDF Bot...")
    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .read_timeout(300)
        .write_timeout(300)
        .connect_timeout(60)
        .pool_timeout(300)
        .build()
    )
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(MessageHandler(filters.Document.ALL, doc_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, other_handler))
    log.info("Bot running. Press Ctrl+C to stop.")
    app.run_polling(allowed_updates=["message"], drop_pending_updates=True)

if __name__ == "__main__":
    main()
