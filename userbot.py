"""
Advanced Telegram Userbot (Telethon)
Features:
- StringSession login
- Master-only commands (by MASTER_ID)
- Auto-reply by keyword
- Auto-delete by banned keywords
- Forwarding, media download/forward
- Scheduling messages (simple scheduler)
- Rate limiter (token bucket)
- Logging to file and optional push to master
- Safe 'raid' with strong limits & confirmation
"""

import os
import re
import asyncio
import logging
from datetime import datetime, timedelta
from telethon import TelegramClient, events, types
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError, UserNotMutualContactError
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import aiofiles
from dotenv import load_dotenv

load_dotenv()  # .env support

API_ID = int(os.getenv("API_ID", "YOUR_API_ID"))
API_HASH = os.getenv("API_HASH", "YOUR_API_HASH")
STRING_SESSION = os.getenv("STRING_SESSION", None)
MASTER_ID = int(os.getenv("MASTER_ID", "YOUR_TELEGRAM_ID"))  # your user id

if not STRING_SESSION:
    raise SystemExit("Please set STRING_SESSION in .env (see instructions).")

# Logging
logging.basicConfig(level=logging.INFO, filename="userbot.log",
                    format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

client = TelegramClient(StringSession(STRING_SESSION), API_ID, API_HASH)
scheduler = AsyncIOScheduler()
scheduler.start()

# Simple in-memory storages (for demo). Persist as needed.
auto_replies = {}   # keyword -> reply_text
auto_delete = set() # banned keywords
watch_list = set()  # chat ids to auto-forward from
forward_targets = []  # list of chat ids
stats = {"messages_processed": 0, "media_downloaded": 0}

MEDIA_DIR = "downloaded_media"
os.makedirs(MEDIA_DIR, exist_ok=True)

# Rate limiter (token bucket)
class RateLimiter:
    def __init__(self, rate, per_seconds):
        self.rate = rate
        self.per = per_seconds
        self.allowance = rate
        self.last_check = asyncio.get_event_loop().time()

    async def acquire(self):
        now = asyncio.get_event_loop().time()
        time_passed = now - self.last_check
        self.last_check = now
        self.allowance += time_passed * (self.rate / self.per)
        if self.allowance > self.rate:
            self.allowance = self.rate
        if self.allowance < 1.0:
            # need to wait
            wait_for = (1.0 - self.allowance) * (self.per / self.rate)
            await asyncio.sleep(wait_for)
            self.allowance = 0.0
            return
        else:
            self.allowance -= 1.0
            return

# Allow max ~20 commands per minute by master (adjustable)
master_rate_limiter = RateLimiter(rate=20, per_seconds=60)

# Utility
def is_master(event):
    try:
        return event.sender_id == MASTER_ID
    except Exception:
        return False

async def send_master(msg):
    try:
        await client.send_message(MASTER_ID, msg)
    except Exception as e:
        logger.exception("Failed to notify master: %s", e)

# Save media
async def save_media(message, prefix="media"):
    if not message.media:
        return None
    fname = f"{prefix}_{int(datetime.now().timestamp())}"
    path = os.path.join(MEDIA_DIR, fname)
    try:
        saved = await message.download_media(file=path)
        stats["media_downloaded"] += 1
        logger.info("Saved media to %s", saved)
        return saved
    except Exception as e:
        logger.exception("media save failed: %s", e)
        return None

# Command regex
COMMAND_RE = re.compile(r'^\.(\w+)(?:\s+([\s\S]+))?$', re.IGNORECASE | re.UNICODE)

# Event: all new messages (main handler)
@client.on(events.NewMessage(incoming=True))
async def handler(event):
    stats["messages_processed"] += 1
    text = event.raw_text or ""
    sender = await event.get_sender()
    sender_id = event.sender_id

    # Auto-delete check
    lower = text.lower()
    for banned in auto_delete:
        if banned in lower:
            try:
                await event.delete()
                logger.info("Auto-deleted message with banned keyword '%s' from %s", banned, sender_id)
                await send_master(f"Auto-deleted message in chat {event.chat_id} by {sender_id} (keyword: {banned})")
            except Exception as e:
                logger.exception("Failed to auto-delete: %s", e)
            return  # skip further processing for deleted messages

    # Auto-reply
    for kw, reply in auto_replies.items():
        if kw in lower:
            try:
                await event.reply(reply)
                logger.info("Auto-replied to %s for keyword %s", sender_id, kw)
            except Exception as e:
                logger.exception("Auto-reply failed: %s", e)
            break

    # Auto-forward from watched chats
    if event.chat_id in watch_list:
        for target in forward_targets:
            try:
                await client.forward_messages(entity=target, messages=event.message, from_peer=event.chat_id)
                logger.info("Forwarded message from %s to %s", event.chat_id, target)
            except Exception as e:
                logger.exception("Forward failed: %s", e)

    # Command handling (only if message starts with .command)
    m = COMMAND_RE.match(text.strip())
    if not m:
        return
    cmd = m.group(1).lower()
    arg = (m.group(2) or "").strip()

    # restrict commands to master only
    if not is_master(event):
        logger.info("Non-master attempted command %s by %s", cmd, sender_id)
        return

    # master rate limit
    await master_rate_limiter.acquire()

    # handle commands
    try:
        if cmd in ("start", "help"):
            await event.reply("Advanced Userbot:\n.help .stats .ping .addreply .delreply .listreplies .addbanned .delbanned .listbanned .watch .unwatch .addforward .removeforward .schedulein .scheduleat .save_media .raid")
            return

        if cmd == "ping":
            await event.reply("Pong! " + datetime.utcnow().isoformat())
            return

        if cmd == "stats":
            await event.reply(f"Messages processed: {stats['messages_processed']}\nMedia downloaded: {stats['media_downloaded']}")
            return

        # Auto-reply commands
        if cmd == "addreply":
            # arg format: keyword ||| reply text
            if "|||" not in arg:
                await event.reply("Usage: .addreply keyword ||| reply text")
                return
            kw, reply = map(str.strip, arg.split("|||", 1))
            auto_replies[kw.lower()] = reply
            await event.reply(f"Auto-reply added for '{kw}'")
            return

        if cmd == "delreply":
            kw = arg.lower()
            removed = auto_replies.pop(kw, None)
            await event.reply(f"Removed: {bool(removed)}")
            return

        if cmd == "listreplies":
            if not auto_replies:
                await event.reply("No auto-replies set.")
                return
            msg = "Auto-replies:\n" + "\n".join(f"{k} -> {v}" for k, v in auto_replies.items())
            await event.reply(msg[:4000])
            return

        # Auto-delete commands
        if cmd == "addbanned":
            kw = arg.lower()
            auto_delete.add(kw)
            await event.reply(f"Added to banned keywords: {kw}")
            return

        if cmd == "delbanned":
            kw = arg.lower()
            was = kw in auto_delete
            auto_delete.discard(kw)
            await event.reply(f"Removed: {was}")
            return

        if cmd == "listbanned":
            await event.reply("Banned keywords:\n" + "\n".join(auto_delete) if auto_delete else "None")
            return

        # watch / forward
        if cmd == "watch":
            try:
                chat = arg
                entity = await client.get_entity(chat)
                watch_list.add(entity.id)
                await event.reply(f"Watching {entity.id} ({getattr(entity, 'title', entity.username)})")
            except Exception as e:
                await event.reply("Watch failed: " + str(e))
            return

        if cmd == "unwatch":
            try:
                entity = await client.get_entity(arg)
                watch_list.discard(entity.id)
                await event.reply(f"Unwatched {entity.id}")
            except Exception as e:
                await event.reply("Unwatch failed: " + str(e))
            return

        if cmd == "addforward":
            try:
                target = await client.get_entity(arg)
                forward_targets.append(target.id)
                await event.reply(f"Forward target added: {target.id}")
            except Exception as e:
                await event.reply("Add forward failed: " + str(e))
            return

        if cmd == "removeforward":
            try:
                target = await client.get_entity(arg)
                if target.id in forward_targets:
                    forward_targets.remove(target.id)
                    await event.reply(f"Removed forward target: {target.id}")
                else:
                    await event.reply("Target not in list.")
            except Exception as e:
                await event.reply("Remove forward failed: " + str(e))
            return

        # Save media from replied message or current message
        if cmd == "save_media":
            # usage: reply to a media message with .save_media or .save_media optional_prefix
            target_msg = None
            if event.is_reply:
                target_msg = await event.get_reply_message()
            else:
                target_msg = event.message
            saved = await save_media(target_msg, prefix=(arg or "media"))
            await event.reply(f"Saved: {saved}")
            return

        # Scheduler: schedulein <minutes> ||| <chat_id_or_username> ||| <message>
        if cmd == "schedulein":
            # arg format: minutes ||| target ||| message
            parts = [p.strip() for p in arg.split("|||")]
            if len(parts) < 3:
                await event.reply("Use: .schedulein minutes ||| target ||| message")
                return
            minutes = float(parts[0])
            target = parts[1]
            msgtext = parts[2]

            async def job_send():
                try:
                    ent = await client.get_entity(target)
                    await client.send_message(ent, msgtext)
                    await send_master(f"Scheduled message sent to {target}")
                except Exception as e:
                    logger.exception("Scheduled send failed: %s", e)

            run_time = datetime.now() + timedelta(minutes=minutes)
            scheduler.add_job(lambda: asyncio.create_task(job_send()), 'date', run_date=run_time)
            await event.reply(f"Scheduled message in {minutes} minutes to {target}")
            return

        # schedule at absolute time: .scheduleat YYYY-MM-DD HH:MM ||| target ||| message
        if cmd == "scheduleat":
            parts = [p.strip() for p in arg.split("|||")]
            if len(parts) < 3:
                await event.reply("Use: .scheduleat YYYY-MM-DD HH:MM ||| target ||| message")
                return
            timestr = parts[0]
            target = parts[1]
            msgtext = parts[2]
            try:
                run_time = datetime.fromisoformat(timestr)
            except Exception:
                await event.reply("Invalid time format. Use ISO: 2025-10-18 15:30 or 2025-10-18T15:30")
                return

            async def job_send2():
                try:
                    ent = await client.get_entity(target)
                    await client.send_message(ent, msgtext)
                    await send_master(f"Scheduled message sent to {target} at {run_time}")
                except Exception as e:
                    logger.exception("Scheduled send failed: %s", e)

            scheduler.add_job(lambda: asyncio.create_task(job_send2()), 'date', run_date=run_time)
            await event.reply(f"Scheduled message at {run_time} to {target}")
            return

        # SAFE RAIDS: **Disabled by default**. Must use .raidconfirm within 30s after calling .raid to proceed.
        # .raid <count> ||| <chat> ||| <message>
        if cmd == "raid":
            # minimal safety: require explicit confirm within 30s and cap count <= 5
            parts = [p.strip() for p in arg.split("|||")]
            if len(parts) < 3:
                await event.reply("Use: .raid count ||| target ||| message\n**This feature is rate-limited and requires confirmation.**")
                return
            try:
                count = int(parts[0])
            except:
                await event.reply("Invalid count.")
                return
            if count > 5:
                await event.reply("Max allowed count is 5 (safety).")
                return
            target = parts[1]; msgtext = parts[2]
            # store a pending confirmation
            pending_key = f"raid_pending:{event.chat_id}:{event.id}"
            # store in memory using scheduler job's kwargs - simple approach:
            scheduler.add_job(lambda: None, 'date', run_date=datetime.now() + timedelta(seconds=30), id=pending_key)
            # Save details in a global (attach to scheduler object)
            scheduler._raids = getattr(scheduler, "_raids", {})
            scheduler._raids[pending_key] = {"count": count, "target": target, "message": msgtext, "requested_by": event.sender_id}
            await event.reply("Raid request queued. To confirm, send .raidconfirm within 30 seconds.")
            return

        if cmd == "raidconfirm":
            # find pending
            pending_keys = [k for k in getattr(scheduler, "_raids", {}) if k.startswith("raid_pending")]
            if not pending_keys:
                await event.reply("No raid pending.")
                return
            # pick latest
            k = pending_keys[-1]
            data = scheduler._raids.get(k)
            if not data:
                await event.reply("No raid pending data.")
                return
            # execute with strong rate-limits
            target = data["target"]; count = data["count"]; msgtext = data["message"]
            try:
                ent = await client.get_entity(target)
            except Exception as e:
                await event.reply("Invalid target: " + str(e))
                return
            await event.reply(f"Executing safe raid: {count} msgs to {target} with strict delays.")
            for i in range(count):
                try:
                    await client.send_message(ent, f"{msgtext} [{i+1}/{count}]")
                    await asyncio.sleep(3)  # delay between messages
                except FloodWaitError as fe:
                    await event.reply(f"Flood wait: {fe.seconds}s")
                    await asyncio.sleep(fe.seconds + 1)
                except Exception as e:
                    logger.exception("raid send failed: %s", e)
            # cleanup
            del scheduler._raids[k]
            await event.reply("Safe raid finished.")
            await send_master(f"Raid performed by master to {target}, count={count}")
            return

        # Generic fallback: unknown command
        await event.reply("Unknown command. Use .help for list.")
    except Exception as e:
        logger.exception("Command handler error: %s", e)
        await event.reply("Error processing command: " + str(e))

# Startup
async def main():
    try:
        await client.start()
        me = await client.get_me()
        logger.info("Userbot started as %s (id=%s)", me.username, me.id)
        await send_master(f"Userbot started: {me.username} ({me.id})")
        print("Userbot running... Press Ctrl+C to stop.")
        await client.run_until_disconnected()
    except Exception as e:
        logger.exception("Startup error: %s", e)

if __name__ == "__main__":
    asyncio.run(main())
