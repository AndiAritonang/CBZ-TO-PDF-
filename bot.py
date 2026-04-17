import os, re, logging, asyncio, zipfile, tempfile, shutil, time
from pathlib import Path
from PIL import Image
import img2pdf
from pyrogram import Client, filters
from pyrogram.types import Message, ReactionTypeEmoji
from pyrogram.errors import FloodWait

# ── CONFIG ─────────────────────────────────────────────────────────────────────
API_ID    = int(os.environ.get("API_ID",    "37623239"))
API_HASH  =     os.environ.get("API_HASH",  "9661c0bdbd8392709dd93139e8c3afcb")
BOT_TOKEN =     os.environ.get("BOT_TOKEN", "8663170411:AAEer7ziKHmqIg1TZ-7QN_jzSd17aH6gNfc")
SUPPORTED = {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".tiff", ".tif", ".gif"}
MAX_RETRIES = 3

logging.basicConfig(format="%(asctime)s | %(levelname)s | %(message)s", level=logging.INFO)
log = logging.getLogger(__name__)

app = Client("cbz_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# ── HELPERS ────────────────────────────────────────────────────────────────────
def bar(pct: int) -> str:
    return "█" * int(pct / 10) + "░" * (10 - int(pct / 10))

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

def natural_key(name: str) -> list:
    return [int(c) if c.isdigit() else c.lower() for c in re.split(r'(\d+)', name)]

# ── CBZ EXTRACTION ─────────────────────────────────────────────────────────────
def extract_cbz(cbz_path: Path, out_dir: Path) -> list:
    # Check file is not empty
    if cbz_path.stat().st_size < 100:
        raise ValueError("Downloaded file is too small — possibly corrupted.")

    # Try to repair/open the zip
    if not zipfile.is_zipfile(cbz_path):
        raise ValueError("Not a valid CBZ/ZIP file.")

    with zipfile.ZipFile(cbz_path, "r") as zf:
        for name in zf.namelist():
            if ".." in name or name.startswith("/"):
                raise ValueError("Unsafe path detected in archive.")
        zf.extractall(out_dir)

    images = [
        p for p in out_dir.rglob("*")
        if p.is_file() and p.suffix.lower() in SUPPORTED
    ]
    if not images:
        raise ValueError("No supported images found inside CBZ.")
    return sorted(images, key=lambda p: natural_key(p.name))

# ── PDF CONVERSION ─────────────────────────────────────────────────────────────
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

# ── PROCESS ONE FILE (runs in parallel with others) ────────────────────────────
async def process_one(message: Message) -> None:
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
        if now - last_edit[0] < 3.0:
            return
        last_edit[0] = now
        pct = max(5, int((current / total) * 30)) if total else 5
        mb_now   = current / 1024 / 1024
        mb_total = total   / 1024 / 1024
        await safe_edit(status, make_text("📥 Downloading...", pct, fname, f"{mb_now:.1f} / {mb_total:.1f} MB"))

    async def ul_progress(current, total):
        now = time.time()
        if now - last_edit[0] < 3.0:
            return
        last_edit[0] = now
        pct = 90 + int((current / total) * 9) if total else 92
        mb_now   = current / 1024 / 1024
        mb_total = total   / 1024 / 1024
        await safe_edit(status, make_text("📤 Uploading PDF...", pct, fname, f"{mb_now:.1f} / {mb_total:.1f} MB"))

    try:
        # ── 1. DOWNLOAD (with retry) ───────────────────────────────────────────
        await safe_edit(status, make_text("📥 Downloading...", 5, fname))

        download_ok = False
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                await app.download_media(message, file_name=str(cbz_path), progress=dl_progress)
                # Verify file was actually downloaded
                if cbz_path.exists() and cbz_path.stat().st_size > 100:
                    download_ok = True
                    break
                else:
                    log.warning(f"Attempt {attempt}: file too small, retrying...")
                    await asyncio.sleep(2 * attempt)
            except Exception as e:
                log.warning(f"Download attempt {attempt} failed: {e}")
                await asyncio.sleep(2 * attempt)

        if not download_ok:
            raise ValueError("Download failed after 3 attempts. Please resend the file.")

        await safe_edit(status, make_text("📥 Download complete!", 30, fname))

        # ── 2. EXTRACT ────────────────────────────────────────────────────────
        await safe_edit(status, make_text("📂 Extracting CBZ...", 40, fname))
        loop = asyncio.get_event_loop()
        images = await loop.run_in_executor(None, extract_cbz, cbz_path, extract_dir)
        page_count = len(images)
        await safe_edit(status, make_text("📂 Extracted!", 55, fname, f"{page_count} pages found"))

        # ── 3. PROCESS + CONVERT ──────────────────────────────────────────────
        await safe_edit(status, make_text("🖼️ Processing images...", 65, fname, f"Converting {page_count} pages"))
        await loop.run_in_executor(None, convert_to_pdf, images, pdf_path)
        pdf_mb = pdf_path.stat().st_size / 1024 / 1024
        await safe_edit(status, make_text("📄 PDF ready!", 90, fname, f"Size: {pdf_mb:.1f} MB"))

        # ── 4. UPLOAD ─────────────────────────────────────────────────────────
        last_edit[0] = 0.0
        await app.send_document(
            chat_id=chat_id,
            document=str(pdf_path),
            file_name=pdf_name,
            caption=(
                f"✅ **{pdf_name}**\n"
                f"📄 Pages: **{page_count}**\n"
                f"📦 Size: **{pdf_mb:.1f} MB**"
            ),
            progress=ul_progress,
        )
        await safe_edit(status, make_text("✅ Done!", 100, fname, "PDF sent successfully"))

    except zipfile.BadZipFile:
        await safe_edit(status, f"❌ **{fname}**\n\nCorrupt ZIP. Please resend.")
    except ValueError as e:
        await safe_edit(status, f"❌ **{fname}**\n\n{e}")
    except Exception as e:
        log.exception(f"Unexpected error: {fname}")
        await safe_edit(status, f"❌ **{fname}**\n\nError: `{type(e).__name__}: {e}`")
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)
        log.info(f"Done & cleaned: {fname}")

# ── HANDLERS ───────────────────────────────────────────────────────────────────
@app.on_message(filters.command("start"))
async def start_cmd(client: Client, message: Message) -> None:
    await react(message)
    await message.reply_text(
        "Iam PArshyas CBZ TO PDF bot...!!!\n\n"
        "⚡ **CBZ → PDF Bot**\n\n"
        "Send me .cbz files — one or many....!!!\n"
        "I'll convert each to PDF and send it back...!!!\n\n"
        "✅ All files processed **simultaneously**\n"
        "✅ Real-time progress per file\n"
        "✅ Supports large files up to **2GB**\n"
        "✅ Auto cleanup after every job\n\n"
        "Drop your CBZ files below ⬇️"
    )

@app.on_message(filters.document)
async def doc_handler(client: Client, message: Message) -> None:
    doc   = message.document
    fname = (doc.file_name or "").lower()

    await react(message)

    if not fname.endswith(".cbz"):
        await message.reply_text("⚠️ Please send `.cbz` files only.")
        return

    # Fire and forget — all files run in parallel
    asyncio.create_task(process_one(message))

@app.on_message(filters.text & ~filters.command("start"))
async def text_handler(client: Client, message: Message) -> None:
    await react(message)
    await message.reply_text(
        "Iam PArshyas CBZ TO PDF bot...!!!\n\n"
        "⚡ **CBZ → PDF Bot**\n\n"
        "Send me .cbz files — one or many....!!!\n"
        "I'll convert each to PDF and send it back...!!!"
    )

# ── MAIN ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    log.info("CBZ → PDF Bot starting — parallel mode (Pyrogram MTProto)...")
    app.run() 