import os, re, logging, asyncio, zipfile, tempfile, shutil, time
from pathlib import Path
from collections import defaultdict
from PIL import Image
from pyrogram import Client, filters
from pyrogram.errors import FloodWait

# ── CONFIG
API_ID = int(os.environ.get("API_ID", "37623239"))
API_HASH = os.environ.get("API_HASH", "9661c0bdbd8392709dd93139e8c3afcb")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "PASTE_NEW_TOKEN")

SUPPORTED = {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".tiff", ".tif", ".gif"}

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO
)

log = logging.getLogger(__name__)

app = Client(
    "cbz_session",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    sleep_threshold=60,
    max_concurrent_transmissions=4,
    in_memory=False,
)

DOWNLOAD_SEM = asyncio.Semaphore(2)

user_queues = defaultdict(asyncio.Queue)
user_workers = {}

# ── UI
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
    except:
        pass

async def safe_delete(msg):
    try:
        await msg.delete()
    except:
        pass

# ── SORT
def natural_key(name):
    return [int(c) if c.isdigit() else c.lower() for c in re.split(r'(\d+)', name)]

# ── ZIP CHECK
def zip_is_openable(path):
    try:
        with zipfile.ZipFile(path, "r") as zf:
            _ = zf.namelist()
        return True
    except:
        return False

# ── EXTRACT
def extract_cbz(cbz_path, out_dir):
    with zipfile.ZipFile(cbz_path, "r") as zf:
        for member in zf.namelist():
            if ".." in member or member.startswith("/"):
                continue

            try:
                zf.extract(member, out_dir)
            except:
                pass

    images = [
        p for p in out_dir.rglob("*")
        if p.is_file() and p.suffix.lower() in SUPPORTED
    ]

    if not images:
        raise ValueError("No supported images found.")

    return sorted(images, key=lambda p: natural_key(p.name))

# ── STABLE PDF CONVERT
def convert_to_pdf(images, pdf_path):
    rgb_images = []

    for img_path in images:
        try:
            img = Image.open(img_path).convert("RGB")
            rgb_images.append(img)
        except Exception as e:
            log.warning(f"Skipping {img_path.name}: {e}")

    if not rgb_images:
        raise ValueError("No valid images.")

    rgb_images[0].save(
        pdf_path,
        save_all=True,
        append_images=rgb_images[1:],
        format="PDF",
        optimize=True
    )

    for img in rgb_images:
        img.close()

# ── DOWNLOAD
async def do_download(message, cbz_path, status, fname):
    expected = message.document.file_size or 0
    min_size = max(500, int(expected * 0.95))

    last_edit = [0.0]

    async def dl_progress(current, total):
        now = time.time()

        if now - last_edit[0] < 3:
            return

        last_edit[0] = now

        pct = max(5, int((current / total) * 30)) if total else 5

        await safe_edit(
            status,
            make_text(
                "📥 Downloading...",
                pct,
                fname,
                f"{current/1024/1024:.1f} / {total/1024/1024:.1f} MB"
            )
        )

    async with DOWNLOAD_SEM:
        await app.download_media(
            message,
            file_name=str(cbz_path),
            progress=dl_progress
        )

    actual = cbz_path.stat().st_size if cbz_path.exists() else 0

    if actual < min_size:
        raise ValueError("Incomplete download.")

    if not zip_is_openable(cbz_path):
        raise ValueError("Invalid CBZ.")

# ── PROCESS
async def process_one(message):
    doc = message.document

    fname = doc.file_name or "file.cbz"
    stem = Path(fname).stem
    pdf_name = f"{stem}.pdf"

    chat_id = message.chat.id

    status = await app.send_message(
        chat_id,
        make_text("📥 Starting...", 0, fname)
    )

    work_dir = Path(tempfile.mkdtemp(prefix="cbzbot_"))

    cbz_path = work_dir / fname

    extract_dir = work_dir / "extract"
    extract_dir.mkdir()

    pdf_path = work_dir / pdf_name

    orig_caption = message.caption

    thumb_path = None

    try:
        if doc.thumbs:
            thumb_path = work_dir / "thumb.jpg"

            await app.download_media(
                doc.thumbs[-1],
                file_name=str(thumb_path)
            )
    except:
        thumb_path = None

    try:
        await safe_edit(status, make_text("📥 Downloading...", 5, fname))

        await do_download(message, cbz_path, status, fname)

        await safe_edit(status, make_text("📂 Extracting...", 40, fname))

        loop = asyncio.get_event_loop()

        images = await loop.run_in_executor(
            None,
            extract_cbz,
            cbz_path,
            extract_dir
        )

        page_count = len(images)

        await safe_edit(
            status,
            make_text(
                "🖼️ Converting...",
                70,
                fname,
                f"{page_count} pages → PDF"
            )
        )

        await asyncio.wait_for(
            loop.run_in_executor(
                None,
                convert_to_pdf,
                images,
                pdf_path
            ),
            timeout=900
        )

        await safe_edit(status, make_text("📤 Uploading...", 90, fname))

        await app.send_document(
            chat_id=chat_id,
            document=str(pdf_path),
            file_name=pdf_name,
            caption=orig_caption,
            thumb=str(thumb_path) if thumb_path and thumb_path.exists() else None
        )

        await safe_delete(status)

    except Exception as e:
        log.exception(fname)

        await safe_edit(
            status,
            f"❌ **{fname}**\n\n`{type(e).__name__}`"
        )

        await asyncio.sleep(5)

        await safe_delete(status)

    finally:
        shutil.rmtree(work_dir, ignore_errors=True)

# ── QUEUE
async def queue_worker(chat_id):
    q = user_queues[chat_id]

    while True:
        msg = await q.get()

        try:
            await process_one(msg)

        except Exception as e:
            log.exception(e)

        finally:
            q.task_done()

        if q.empty():
            break

    user_workers.pop(chat_id, None)

# ── START
@app.on_message(filters.command("start"))
async def start_cmd(client, message):
    await message.reply_text(
        "Send CBZ files to convert into PDF."
    )

# ── FILES
@app.on_message(filters.document)
async def doc_handler(client, message):

    if message.sender_chat:
        return

    fname = (message.document.file_name or "").lower()

    if not fname.endswith(".cbz"):
        return

    chat_id = message.chat.id

    await user_queues[chat_id].put(message)

    if chat_id not in user_workers or user_workers[chat_id].done():
        user_workers[chat_id] = asyncio.create_task(
            queue_worker(chat_id)
        )

# ── RUN
if __name__ == "__main__":
    log.info("CBZ → PDF Bot Started")
    app.run()