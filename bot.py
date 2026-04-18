import os, re, logging, asyncio, zipfile, tempfile, shutil, time
from pathlib import Path
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

# Retry config — large batches need longer waits (Telegram upload lag)
DOWNLOAD_RETRIES = 5
RETRY_DELAYS     = [5, 15, 30, 60, 90]   # seconds between attempts

logging.basicConfig(format="%(asctime)s | %(levelname)s | %(message)s", level=logging.INFO)
log = logging.getLogger(__name__)

app = Client("cbz_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# ── PER-USER SEND QUEUE (sequential output in received order) ─────────────────
# seq_counter[chat_id]  = next sequence number to assign
# send_ready[chat_id]   = {seq: (pdf_path, pdf_name, page_count, status_msg)} | "error"
# next_to_send[chat_id] = next sequence number to send
seq_counter:  dict = {}
send_ready:   dict = {}
next_to_send: dict = {}
sender_tasks: dict = {}

# ── HELPERS ────────────────────────────────────────────────────────────────────
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

async def react(message):
    try:
        await message.react(emoji="⚡")
    except Exception:
        pass

def natural_key(name):
    return [int(c) if c.isdigit() else c.lower() for c in re.split(r'(\d+)', name)]

# ── CBZ EXTRACTION ─────────────────────────────────────────────────────────────
def extract_cbz(cbz_path, out_dir):
    size = cbz_path.stat().st_size
    if size < 200:
        raise ValueError(f"File too small ({size} bytes) — not fully downloaded yet.")
    if not zipfile.is_zipfile(cbz_path):
        raise ValueError("Not a valid CBZ/ZIP file.")
    with zipfile.ZipFile(cbz_path, "r") as zf:
        for name in zf.namelist():
            if ".." in name or name.startswith("/"):
                raise ValueError("Unsafe path in archive.")
        zf.extractall(out_dir)
    images = [p for p in out_dir.rglob("*") if p.is_file() and p.suffix.lower() in SUPPORTED]
    if not images:
        raise ValueError("No supported images found inside CBZ.")
    return sorted(images, key=lambda p: natural_key(p.name))

# ── PDF CONVERSION ─────────────────────────────────────────────────────────────
def convert_to_pdf(images, pdf_path):
    safe, temps = [], []
    for img_path in images:
        try:
            with Image.open(img_path) as im:
                if im.mode in ("RGBA", "P", "LA", "L"):
                    conv = img_path.with_suffix("._c.jpg")
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
        imgs = []
        for p in safe:
            try: imgs.append(Image.open(p).convert("RGB"))
            except Exception: pass
        if not imgs:
            raise ValueError("Pillow fallback also failed.")
        imgs[0].save(pdf_path, save_all=True, append_images=imgs[1:], format="PDF")
    finally:
        for p in temps: p.unlink(missing_ok=True)

# ── SENDER: sends PDFs in received order ──────────────────────────────────────
async def ordered_sender(chat_id):
    """Waits for tasks to finish and sends PDFs in original received order."""
    ready = send_ready[chat_id]
    while True:
        seq = next_to_send[chat_id]
        if seq not in ready:
            await asyncio.sleep(0.5)
            # If no new results in 10 min, exit
            continue
        result = ready.pop(seq)
        next_to_send[chat_id] = seq + 1

        if result == "error":
            pass  # status msg already edited with error
        else:
            pdf_path, pdf_name, page_count, status_msg, work_dir = result
            try:
                last_edit = [0.0]
                async def ul_progress(current, total):
                    now = time.time()
                    if now - last_edit[0] < 3.0: return
                    last_edit[0] = now
                    pct = 90 + int((current/total)*9) if total else 92
                    await safe_edit(status_msg, make_text(
                        "📤 Uploading PDF...", pct, pdf_name,
                        f"{current/1024/1024:.1f} / {total/1024/1024:.1f} MB"
                    ))
                pdf_mb = pdf_path.stat().st_size / 1024 / 1024
                await safe_edit(status_msg, make_text("📤 Uploading...", 90, pdf_name, f"{pdf_mb:.1f} MB"))
                await app.send_document(
                    chat_id=chat_id,
                    document=str(pdf_path),
                    file_name=pdf_name,
                    caption=f"✅ **{pdf_name}**\n📄 Pages: **{page_count}**\n📦 Size: **{pdf_mb:.1f} MB**",
                    progress=ul_progress,
                )
                await safe_edit(status_msg, make_text("✅ Done!", 100, pdf_name, "PDF sent successfully"))
            except Exception as e:
                await safe_edit(status_msg, f"❌ Upload failed: `{e}`")
            finally:
                shutil.rmtree(work_dir, ignore_errors=True)

        # Check if all done
        if next_to_send[chat_id] >= seq_counter[chat_id] and not ready:
            break

    # Cleanup
    sender_tasks.pop(chat_id, None)
    send_ready.pop(chat_id, None)
    next_to_send.pop(chat_id, None)
    seq_counter.pop(chat_id, None)
    log.info(f"Sender done for chat {chat_id}")

# ── PROCESS ONE FILE (parallel, download + convert) ───────────────────────────
async def process_one(message, seq):
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

    last_edit = [0.0]

    async def dl_progress(current, total):
        now = time.time()
        if now - last_edit[0] < 3.0: return
        last_edit[0] = now
        pct = max(5, int((current/total)*30)) if total else 5
        await safe_edit(status, make_text(
            "📥 Downloading...", pct, fname,
            f"{current/1024/1024:.1f} / {total/1024/1024:.1f} MB"
        ))

    try:
        # ── DOWNLOAD with retry (handles Telegram upload lag on big batches) ──
        download_ok = False
        for attempt in range(DOWNLOAD_RETRIES):
            try:
                if cbz_path.exists():
                    cbz_path.unlink()
                await app.download_media(message, file_name=str(cbz_path), progress=dl_progress)
                if cbz_path.exists() and cbz_path.stat().st_size > 200 and zipfile.is_zipfile(cbz_path):
                    download_ok = True
                    break
                # File exists but not valid yet — Telegram still uploading
                wait = RETRY_DELAYS[attempt]
                log.warning(f"{fname}: invalid ZIP on attempt {attempt+1}, waiting {wait}s...")
                await safe_edit(status, make_text(
                    f"⏳ Waiting for Telegram... (attempt {attempt+1}/{DOWNLOAD_RETRIES})",
                    5, fname, f"Retry in {wait}s"
                ))
                await asyncio.sleep(wait)
            except Exception as e:
                wait = RETRY_DELAYS[min(attempt, len(RETRY_DELAYS)-1)]
                log.warning(f"{fname} download attempt {attempt+1} error: {e}, waiting {wait}s")
                await asyncio.sleep(wait)

        if not download_ok:
            raise ValueError(f"Could not download after {DOWNLOAD_RETRIES} attempts. Please resend.")

        await safe_edit(status, make_text("📥 Download complete!", 30, fname))

        # ── EXTRACT ───────────────────────────────────────────────────────────
        await safe_edit(status, make_text("📂 Extracting...", 40, fname))
        loop = asyncio.get_event_loop()
        images = await loop.run_in_executor(None, extract_cbz, cbz_path, extract_dir)
        page_count = len(images)
        await safe_edit(status, make_text("📂 Extracted!", 55, fname, f"{page_count} pages"))

        # ── CONVERT ───────────────────────────────────────────────────────────
        await safe_edit(status, make_text("🖼️ Converting...", 70, fname, f"{page_count} pages → PDF"))
        await loop.run_in_executor(None, convert_to_pdf, images, pdf_path)
        await safe_edit(status, make_text("📄 PDF ready! Waiting to send...", 88, fname))

        # Mark as ready for ordered sender
        send_ready[chat_id][seq] = (pdf_path, pdf_name, page_count, status, work_dir)

    except Exception as e:
        log.exception(f"Error: {fname}")
        await safe_edit(status, f"❌ **{fname}**\n\n{e}")
        shutil.rmtree(work_dir, ignore_errors=True)
        send_ready[chat_id][seq] = "error"

# ── HANDLERS ───────────────────────────────────────────────────────────────────
@app.on_message(filters.command("start"))
async def start_cmd(client, message):
    await react(message)
    await message.reply_text(
        "Iam PArshyas CBZ TO PDF bot...!!!\n\n"
        "⚡ **CBZ → PDF Bot**\n\n"
        "Send me .cbz files — one or many....!!!\n"
        "I'll convert each to PDF and send it back...!!!\n\n"
        "✅ All files processed **simultaneously**\n"
        "✅ PDFs sent in the **same order** you sent CBZ files\n"
        "✅ Supports large files up to **2GB**\n"
        "✅ Smart retry if Telegram upload is slow\n\n"
        "Drop your CBZ files below ⬇️"
    )

@app.on_message(filters.document)
async def doc_handler(client, message):
    doc   = message.document
    fname = (doc.file_name or "").lower()
    await react(message)
    if not fname.endswith(".cbz"):
        await message.reply_text("⚠️ Please send `.cbz` files only.")
        return

    chat_id = message.chat.id

    # Init per-chat state
    if chat_id not in seq_counter:
        seq_counter[chat_id]  = 0
        send_ready[chat_id]   = {}
        next_to_send[chat_id] = 0

    seq = seq_counter[chat_id]
    seq_counter[chat_id] += 1

    # Start parallel processing task
    asyncio.create_task(process_one(message, seq))

    # Start ordered sender if not running
    if chat_id not in sender_tasks or sender_tasks[chat_id].done():
        sender_tasks[chat_id] = asyncio.create_task(ordered_sender(chat_id))

@app.on_message(filters.text & ~filters.command("start"))
async def text_handler(client, message):
    await react(message)
    await message.reply_text(
        "Iam PArshyas CBZ TO PDF bot...!!!\n\n"
        "⚡ **CBZ → PDF Bot**\n\n"
        "Send me .cbz files — one or many....!!!\n"
        "I'll convert each to PDF and send it back...!!!"
    )

# ── MAIN ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    log.info("CBZ→PDF Bot — PARALLEL + ORDERED (Pyrogram MTProto)")
    app.run()