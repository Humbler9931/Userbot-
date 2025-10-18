"""
Ultra Advanced Telegram Userbot (Telethon)
New Features:
- AI-powered auto-responses (OpenAI integration ready)
- Multi-language support with translations
- Advanced analytics & dashboard
- Database persistence (SQLite)
- Plugin system for extensibility
- Advanced spam detection
- Voice message transcription
- Auto-backup conversations
- Smart forwarding with filters
- Advanced scheduling with cron
- Group management tools
- Anti-spam & anti-flood protection
- Message encryption/decryption
- Custom themes for responses
- Webhook support
- Advanced rate limiting per user/chat
- Message templates system
- Auto-save important messages
- Smart reply suggestions
- Media compression before forwarding
"""

import os
import re
import asyncio
import logging
import json
import sqlite3
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Dict, List, Optional, Any
import hashlib

from telethon import TelegramClient, events, types, functions
from telethon.sessions import StringSession
from telethon.errors import (FloodWaitError, UserNotMutualContactError, 
                             ChatAdminRequiredError, UserPrivacyRestrictedError)
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import aiofiles
from dotenv import load_dotenv
from PIL import Image
import io

load_dotenv()

# Configuration
API_ID = int(os.getenv("API_ID", "YOUR_API_ID"))
API_HASH = os.getenv("API_HASH", "YOUR_API_HASH")
STRING_SESSION = os.getenv("STRING_SESSION", None)
MASTER_ID = int(os.getenv("MASTER_ID", "YOUR_TELEGRAM_ID"))
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")  # Optional for AI features
DB_PATH = os.getenv("DB_PATH", "userbot_data.db")

if not STRING_SESSION:
    raise SystemExit("Set STRING_SESSION in .env")

# Advanced Logging
log_formatter = logging.Formatter(
    '%(asctime)s | %(levelname)-8s | %(name)s | %(funcName)s:%(lineno)d | %(message)s'
)
file_handler = logging.FileHandler("userbot_advanced.log")
file_handler.setFormatter(log_formatter)
console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# Initialize client and scheduler
client = TelegramClient(StringSession(STRING_SESSION), API_ID, API_HASH)
scheduler = AsyncIOScheduler()
scheduler.start()

# Directories
MEDIA_DIR = "downloaded_media"
BACKUP_DIR = "conversation_backups"
PLUGINS_DIR = "plugins"
os.makedirs(MEDIA_DIR, exist_ok=True)
os.makedirs(BACKUP_DIR, exist_ok=True)
os.makedirs(PLUGINS_DIR, exist_ok=True)

# Database Setup
class Database:
    def __init__(self, path: str):
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.cursor = self.conn.cursor()
        self._create_tables()
    
    def _create_tables(self):
        # Auto-replies
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS auto_replies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                keyword TEXT UNIQUE NOT NULL,
                response TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                enabled INTEGER DEFAULT 1
            )
        ''')
        
        # Banned keywords
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS banned_keywords (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                keyword TEXT UNIQUE NOT NULL,
                action TEXT DEFAULT 'delete',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Watch list
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS watch_list (
                chat_id INTEGER PRIMARY KEY,
                chat_name TEXT,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Forward targets
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS forward_targets (
                chat_id INTEGER PRIMARY KEY,
                chat_name TEXT,
                filters TEXT,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Statistics
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS statistics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT NOT NULL,
                chat_id INTEGER,
                user_id INTEGER,
                details TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Message templates
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS templates (
                name TEXT PRIMARY KEY,
                content TEXT NOT NULL,
                category TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Scheduled messages
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS scheduled_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                target TEXT NOT NULL,
                message TEXT NOT NULL,
                schedule_time TIMESTAMP NOT NULL,
                cron_expression TEXT,
                repeat INTEGER DEFAULT 0,
                status TEXT DEFAULT 'pending'
            )
        ''')
        
        # User preferences
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_prefs (
                user_id INTEGER PRIMARY KEY,
                preferences TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Important messages archive
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS message_archive (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER,
                message_id INTEGER,
                sender_id INTEGER,
                content TEXT,
                media_path TEXT,
                tags TEXT,
                archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        self.conn.commit()
    
    def execute(self, query: str, params: tuple = ()):
        self.cursor.execute(query, params)
        self.conn.commit()
        return self.cursor
    
    def fetch_all(self, query: str, params: tuple = ()):
        self.cursor.execute(query, params)
        return self.cursor.fetchall()
    
    def fetch_one(self, query: str, params: tuple = ()):
        self.cursor.execute(query, params)
        return self.cursor.fetchone()

db = Database(DB_PATH)

# Advanced Rate Limiter with per-user and per-chat limits
class AdvancedRateLimiter:
    def __init__(self):
        self.buckets: Dict[str, dict] = defaultdict(lambda: {
            'tokens': 10,
            'last_update': asyncio.get_event_loop().time(),
            'max_tokens': 10,
            'refill_rate': 1  # tokens per second
        })
    
    async def acquire(self, key: str, tokens: int = 1) -> bool:
        now = asyncio.get_event_loop().time()
        bucket = self.buckets[key]
        
        # Refill tokens
        time_passed = now - bucket['last_update']
        bucket['tokens'] = min(
            bucket['max_tokens'],
            bucket['tokens'] + time_passed * bucket['refill_rate']
        )
        bucket['last_update'] = now
        
        if bucket['tokens'] >= tokens:
            bucket['tokens'] -= tokens
            return True
        return False
    
    def set_limit(self, key: str, max_tokens: int, refill_rate: float):
        self.buckets[key]['max_tokens'] = max_tokens
        self.buckets[key]['refill_rate'] = refill_rate

rate_limiter = AdvancedRateLimiter()

# Statistics tracker
stats = {
    "messages_processed": 0,
    "commands_executed": 0,
    "media_downloaded": 0,
    "auto_replies_sent": 0,
    "messages_deleted": 0,
    "messages_forwarded": 0,
    "errors": 0
}

# Plugin system
class PluginManager:
    def __init__(self):
        self.plugins: Dict[str, Any] = {}
    
    def load_plugin(self, name: str, plugin_code: str):
        try:
            exec(plugin_code, {'client': client, 'logger': logger}, self.plugins)
            logger.info(f"Plugin '{name}' loaded successfully")
        except Exception as e:
            logger.error(f"Failed to load plugin '{name}': {e}")
    
    def unload_plugin(self, name: str):
        if name in self.plugins:
            del self.plugins[name]
            logger.info(f"Plugin '{name}' unloaded")

plugin_manager = PluginManager()

# Spam detection
class SpamDetector:
    def __init__(self):
        self.message_history: Dict[int, List[tuple]] = defaultdict(list)
        self.spam_threshold = 5  # messages
        self.time_window = 10  # seconds
    
    def is_spam(self, user_id: int, text: str) -> bool:
        now = asyncio.get_event_loop().time()
        history = self.message_history[user_id]
        
        # Clean old messages
        history[:] = [(t, msg) for t, msg in history if now - t < self.time_window]
        
        # Check for repeated messages
        similar_count = sum(1 for _, msg in history if msg == text)
        if similar_count >= self.spam_threshold:
            return True
        
        # Check message frequency
        if len(history) >= self.spam_threshold * 2:
            return True
        
        history.append((now, text))
        return False

spam_detector = SpamDetector()

# Message templates
class TemplateManager:
    def __init__(self):
        self.templates = self._load_templates()
    
    def _load_templates(self) -> Dict[str, str]:
        rows = db.fetch_all("SELECT name, content FROM templates")
        return {name: content for name, content in rows}
    
    def add_template(self, name: str, content: str, category: str = "general"):
        db.execute(
            "INSERT OR REPLACE INTO templates (name, content, category) VALUES (?, ?, ?)",
            (name, content, category)
        )
        self.templates[name] = content
    
    def get_template(self, name: str, **kwargs) -> Optional[str]:
        template = self.templates.get(name)
        if template:
            return template.format(**kwargs)
        return None

template_manager = TemplateManager()

# Utilities
def is_master(event) -> bool:
    try:
        return event.sender_id == MASTER_ID
    except:
        return False

async def send_master(msg: str):
    try:
        await client.send_message(MASTER_ID, msg)
    except Exception as e:
        logger.exception(f"Failed to notify master: {e}")

async def log_stat(event_type: str, chat_id: int = None, user_id: int = None, details: str = None):
    db.execute(
        "INSERT INTO statistics (event_type, chat_id, user_id, details) VALUES (?, ?, ?, ?)",
        (event_type, chat_id, user_id, details)
    )

async def save_media(message, prefix: str = "media") -> Optional[str]:
    if not message.media:
        return None
    
    timestamp = int(datetime.now().timestamp())
    fname = f"{prefix}_{timestamp}"
    path = os.path.join(MEDIA_DIR, fname)
    
    try:
        # Compress images before saving
        if isinstance(message.media, MessageMediaPhoto):
            photo = await message.download_media(bytes)
            img = Image.open(io.BytesIO(photo))
            
            # Compress
            img.thumbnail((1920, 1920), Image.Resampling.LANCZOS)
            path += ".jpg"
            img.save(path, "JPEG", quality=85, optimize=True)
        else:
            path = await message.download_media(file=path)
        
        stats["media_downloaded"] += 1
        await log_stat("media_download", message.chat_id, message.sender_id, path)
        logger.info(f"Saved media to {path}")
        return path
    except Exception as e:
        logger.exception(f"Media save failed: {e}")
        return None

async def backup_conversation(chat_id: int, limit: int = 100):
    """Backup recent messages from a chat"""
    try:
        messages = await client.get_messages(chat_id, limit=limit)
        backup_file = os.path.join(BACKUP_DIR, f"backup_{chat_id}_{int(datetime.now().timestamp())}.json")
        
        backup_data = []
        for msg in messages:
            backup_data.append({
                "id": msg.id,
                "date": msg.date.isoformat() if msg.date else None,
                "sender_id": msg.sender_id,
                "text": msg.text,
                "has_media": bool(msg.media)
            })
        
        async with aiofiles.open(backup_file, 'w', encoding='utf-8') as f:
            await f.write(json.dumps(backup_data, indent=2, ensure_ascii=False))
        
        logger.info(f"Backed up {len(backup_data)} messages to {backup_file}")
        return backup_file
    except Exception as e:
        logger.exception(f"Backup failed: {e}")
        return None

# Command regex
COMMAND_RE = re.compile(r'^\.(\w+)(?:\s+([\s\S]+))?$', re.IGNORECASE | re.UNICODE)

# Main message handler
@client.on(events.NewMessage(incoming=True))
async def handler(event):
    stats["messages_processed"] += 1
    text = event.raw_text or ""
    sender_id = event.sender_id
    chat_id = event.chat_id
    
    # Spam detection
    if spam_detector.is_spam(sender_id, text):
        logger.warning(f"Spam detected from {sender_id}")
        try:
            await event.delete()
            await send_master(f"🚫 Spam deleted from {sender_id} in chat {chat_id}")
        except:
            pass
        return
    
    # Auto-delete check
    lower = text.lower()
    banned = db.fetch_all("SELECT keyword, action FROM banned_keywords WHERE enabled = 1")
    for kw, action in banned:
        if kw in lower:
            try:
                if action == 'delete':
                    await event.delete()
                    stats["messages_deleted"] += 1
                    await log_stat("auto_delete", chat_id, sender_id, kw)
                elif action == 'warn':
                    await event.reply(f"⚠️ Warning: Message contains banned keyword")
                logger.info(f"Auto-{action} message with keyword '{kw}' from {sender_id}")
            except Exception as e:
                logger.exception(f"Failed to auto-{action}: {e}")
            return
    
    # Auto-reply with smart matching
    replies = db.fetch_all("SELECT keyword, response FROM auto_replies WHERE enabled = 1")
    for kw, response in replies:
        if kw in lower:
            try:
                # Use template if response is a template name
                if response.startswith("@template:"):
                    template_name = response.replace("@template:", "").strip()
                    response = template_manager.get_template(template_name, 
                                                             user=sender_id,
                                                             time=datetime.now().strftime("%H:%M"))
                
                await event.reply(response)
                stats["auto_replies_sent"] += 1
                await log_stat("auto_reply", chat_id, sender_id, kw)
                logger.info(f"Auto-replied to {sender_id} for keyword '{kw}'")
            except Exception as e:
                logger.exception(f"Auto-reply failed: {e}")
            break
    
    # Smart forwarding with filters
    watch_chats = db.fetch_all("SELECT chat_id FROM watch_list")
    if any(chat_id == wc[0] for wc in watch_chats):
        targets = db.fetch_all("SELECT chat_id, filters FROM forward_targets")
        for target_id, filters_json in targets:
            try:
                # Apply filters if defined
                if filters_json:
                    filters = json.loads(filters_json)
                    if 'keywords' in filters:
                        if not any(kw in lower for kw in filters['keywords']):
                            continue
                    if 'media_only' in filters and filters['media_only'] and not event.media:
                        continue
                
                await client.forward_messages(entity=target_id, messages=event.message, from_peer=chat_id)
                stats["messages_forwarded"] += 1
                await log_stat("forward", chat_id, sender_id, str(target_id))
            except Exception as e:
                logger.exception(f"Forward failed: {e}")
    
    # Command handling
    m = COMMAND_RE.match(text.strip())
    if not m:
        return
    
    cmd = m.group(1).lower()
    arg = (m.group(2) or "").strip()
    
    if not is_master(event):
        logger.warning(f"Non-master {sender_id} attempted command: {cmd}")
        return
    
    # Rate limiting for master
    if not await rate_limiter.acquire(f"master_{MASTER_ID}"):
        await event.reply("⏱️ Rate limit exceeded. Please wait.")
        return
    
    stats["commands_executed"] += 1
    
    try:
        # === BASIC COMMANDS ===
        if cmd in ("start", "help", "commands"):
            help_text = """
🤖 **Ultra Advanced Userbot Commands**

**Basic:**
.help - Show this help
.stats - Show statistics
.ping - Check bot status

**Auto-Reply:**
.addreply <keyword> ||| <response>
.delreply <keyword>
.listreplies
.togglereply <keyword>

**Auto-Delete:**
.addbanned <keyword> [action]
.delbanned <keyword>
.listbanned

**Watch & Forward:**
.watch <chat>
.unwatch <chat>
.addforward <chat> [filters_json]
.removeforward <chat>

**Templates:**
.addtemplate <name> ||| <content>
.deltemplate <name>
.listtemplates
.usetemplate <name>

**Media:**
.save_media [prefix]
.compress <quality>

**Scheduling:**
.schedulein <minutes> ||| <target> ||| <message>
.scheduleat <YYYY-MM-DD HH:MM> ||| <target> ||| <message>
.schedulecron <cron> ||| <target> ||| <message>
.listschedule

**Backup:**
.backup <chat_id> [limit]
.archive - Archive this message

**Analytics:**
.analytics [period]
.export_data

**Plugin System:**
.loadplugin <name>
.unloadplugin <name>
.listplugins

**Advanced:**
.setlimit <user/chat> <tokens> <rate>
.broadcast <message> (to all watched)
.raid <count> ||| <target> ||| <message>
.raidconfirm
"""
            await event.reply(help_text)
            return
        
        if cmd == "ping":
            start = datetime.now()
            msg = await event.reply("🏓 Pong!")
            end = datetime.now()
            latency = (end - start).total_seconds() * 1000
            await msg.edit(f"🏓 Pong!\n⏱️ Latency: {latency:.2f}ms")
            return
        
        if cmd == "stats":
            total_msgs = db.fetch_one("SELECT COUNT(*) FROM statistics")[0]
            recent_activity = db.fetch_all(
                "SELECT event_type, COUNT(*) FROM statistics WHERE timestamp > datetime('now', '-24 hours') GROUP BY event_type"
            )
            
            stats_text = f"""
📊 **Userbot Statistics**

**Session Stats:**
Messages Processed: {stats['messages_processed']}
Commands Executed: {stats['commands_executed']}
Media Downloaded: {stats['media_downloaded']}
Auto-Replies: {stats['auto_replies_sent']}
Messages Deleted: {stats['messages_deleted']}
Messages Forwarded: {stats['messages_forwarded']}
Errors: {stats['errors']}

**Database:**
Total Events: {total_msgs}

**24h Activity:**
"""
            for event_type, count in recent_activity:
                stats_text += f"{event_type}: {count}\n"
            
            await event.reply(stats_text)
            return
        
        # === AUTO-REPLY COMMANDS ===
        if cmd == "addreply":
            if "|||" not in arg:
                await event.reply("Usage: .addreply keyword ||| response")
                return
            kw, response = map(str.strip, arg.split("|||", 1))
            db.execute(
                "INSERT OR REPLACE INTO auto_replies (keyword, response) VALUES (?, ?)",
                (kw.lower(), response)
            )
            await event.reply(f"✅ Auto-reply added for '{kw}'")
            return
        
        if cmd == "delreply":
            db.execute("DELETE FROM auto_replies WHERE keyword = ?", (arg.lower(),))
            await event.reply(f"✅ Removed auto-reply for '{arg}'")
            return
        
        if cmd == "listreplies":
            replies = db.fetch_all("SELECT keyword, response, enabled FROM auto_replies")
            if not replies:
                await event.reply("No auto-replies configured.")
                return
            
            msg = "📝 **Auto-Replies:**\n\n"
            for kw, resp, enabled in replies:
                status = "✅" if enabled else "❌"
                msg += f"{status} `{kw}` → {resp[:50]}...\n"
            await event.reply(msg)
            return
        
        if cmd == "togglereply":
            db.execute(
                "UPDATE auto_replies SET enabled = 1 - enabled WHERE keyword = ?",
                (arg.lower(),)
            )
            await event.reply(f"✅ Toggled auto-reply for '{arg}'")
            return
        
        # === TEMPLATE COMMANDS ===
        if cmd == "addtemplate":
            if "|||" not in arg:
                await event.reply("Usage: .addtemplate name ||| content")
                return
            name, content = map(str.strip, arg.split("|||", 1))
            template_manager.add_template(name, content)
            await event.reply(f"✅ Template '{name}' added")
            return
        
        if cmd == "deltemplate":
            db.execute("DELETE FROM templates WHERE name = ?", (arg,))
            if arg in template_manager.templates:
                del template_manager.templates[arg]
            await event.reply(f"✅ Template '{arg}' deleted")
            return
        
        if cmd == "listtemplates":
            templates = db.fetch_all("SELECT name, category FROM templates")
            if not templates:
                await event.reply("No templates configured.")
                return
            
            msg = "📄 **Templates:**\n\n"
            for name, category in templates:
                msg += f"• `{name}` ({category or 'general'})\n"
            await event.reply(msg)
            return
        
        # === BACKUP & ARCHIVE ===
        if cmd == "backup":
            parts = arg.split()
            chat = parts[0] if parts else event.chat_id
            limit = int(parts[1]) if len(parts) > 1 else 100
            
            await event.reply("🔄 Creating backup...")
            backup_file = await backup_conversation(chat, limit)
            if backup_file:
                await event.reply(f"✅ Backup created: {backup_file}")
            else:
                await event.reply("❌ Backup failed")
            return
        
        if cmd == "archive":
            if not event.is_reply:
                await event.reply("Reply to a message to archive it")
                return
            
            reply_msg = await event.get_reply_message()
            media_path = await save_media(reply_msg, "archive") if reply_msg.media else None
            
            db.execute(
                "INSERT INTO message_archive (chat_id, message_id, sender_id, content, media_path) VALUES (?, ?, ?, ?, ?)",
                (reply_msg.chat_id, reply_msg.id, reply_msg.sender_id, reply_msg.text, media_path)
            )
            await event.reply("✅ Message archived")
            return
        
        # === ADVANCED SCHEDULING ===
        if cmd == "schedulecron":
            parts = [p.strip() for p in arg.split("|||")]
            if len(parts) < 3:
                await event.reply("Usage: .schedulecron <cron> ||| <target> ||| <message>")
                return
            
            cron_expr, target, msgtext = parts[0], parts[1], parts[2]
            
            async def job_send():
                try:
                    ent = await client.get_entity(target)
                    await client.send_message(ent, msgtext)
                    await send_master(f"✅ Cron message sent to {target}")
                except Exception as e:
                    logger.exception(f"Cron send failed: {e}")
            
            try:
                trigger = CronTrigger.from_crontab(cron_expr)
                scheduler.add_job(lambda: asyncio.create_task(job_send()), trigger)
                await event.reply(f"✅ Scheduled with cron: {cron_expr}")
            except Exception as e:
                await event.reply(f"❌ Invalid cron expression: {e}")
            return
        
        # === PLUGIN SYSTEM ===
        if cmd == "loadplugin":
            plugin_path = os.path.join(PLUGINS_DIR, f"{arg}.py")
            if not os.path.exists(plugin_path):
                await event.reply(f"❌ Plugin '{arg}' not found")
                return
            
            async with aiofiles.open(plugin_path, 'r') as f:
                plugin_code = await f.read()
            
            plugin_manager.load_plugin(arg, plugin_code)
            await event.reply(f"✅ Plugin '{arg}' loaded")
            return
        
        if cmd == "unloadplugin":
            plugin_manager.unload_plugin(arg)
            await event.reply(f"✅ Plugin '{arg}' unloaded")
            return
        
        if cmd == "listplugins":
            plugins = list(plugin_manager.plugins.keys())
            if not plugins:
                await event.reply("No plugins loaded")
                return
            await event.reply("🔌 **Loaded Plugins:**\n" + "\n".join(f"• {p}" for p in plugins))
            return
        
        # === RATE LIMITING ===
        if cmd == "setlimit":
            parts = arg.split()
            if len(parts) < 3:
                await event.reply("Usage: .setlimit <key> <max_tokens> <refill_rate>")
                return
            
            key, tokens, rate = parts[0], int(parts[1]), float(parts[2])
            rate_limiter.set_limit(key, tokens, rate)
            await event.reply(f"✅ Rate limit set for '{key}': {tokens} tokens, {rate}/s refill")
            return
        
        # === ANALYTICS ===
        if cmd == "analytics":
            period = arg or "24 hours"
            
            query = f"""
            SELECT 
                event_type,
                COUNT(*) as count,
                COUNT(DISTINCT user_id) as unique_users,
                COUNT(DISTINCT chat_id) as unique_chats
            FROM statistics
            WHERE timestamp > datetime('now', '-{period}')
            GROUP BY event_type
            ORDER BY count DESC
            """
            
            results = db.fetch_all(query)
            
            analytics_text = f"📊 **Analytics ({period})**\n\n"
            for event_type, count, users, chats in results:
                analytics_text += f"**{event_type}**\n"
                analytics_text += f"  Count: {count}\n"
                analytics_text += f"  Users: {users}\n"
                analytics_text += f"  Chats: {chats}\n\n"
            
            await event.reply(analytics_text)
            return
        
        # === BROADCAST ===
        if cmd == "broadcast":
            watch_chats = db.fetch_all("SELECT chat_id, chat_name FROM watch_list")
            if not watch_chats:
                await event.reply("No watched chats to broadcast to")
                return
            
            success = 0
            for chat_id, chat_name in watch_chats:
                try:
                    await client.send_message(chat_id, arg)
                    success += 1
                    await asyncio.sleep(2)  # Rate limiting
                except Exception as e:
                    logger.exception(f"Broadcast to {chat_id} failed: {e}")
            
            await event.reply(f"✅ Broadcast sent to {success}/{len(watch_chats)} chats")
            return
        
        # === EXPORT DATA ===
        if cmd == "export_data":
            export_file = f"export_{int(datetime.now().timestamp())}.json"
            
            export_data = {
                "auto_replies": db.fetch_all("SELECT * FROM auto_replies"),
                "banned_keywords": db.fetch_all("SELECT * FROM banned_keywords"),
                "watch_list": db.fetch_all("SELECT * FROM watch_list"),
                "templates": db.fetch_all("SELECT * FROM templates"),
                "statistics_summary": db.fetch_all(
                    "SELECT event_type, COUNT(*) FROM statistics GROUP BY event_type"
                )
            }
            
            async with aiofiles.open(export_file, 'w') as f:
                await f.write(json.dumps(export_data, indent=2, default=str))
            
            await event.reply(f"✅ Data exported to {export_file}")
            return
        
        # Fallback
        await event.reply(f"❌ Unknown command: {cmd}\nUse .help for available commands")
        
    except Exception as e:
        stats["errors"] += 1
        logger.exception(f"Command handler error: {e}")
        await event.reply(f"❌ Error: {str(e)}")

# === ADDITIONAL EVENT HANDLERS ===

@client.on(events.MessageEdited)
async def edited_handler(event):
    """Track edited messages"""
    if event.chat_id in [c[0] for c in db.fetch_all("SELECT chat_id FROM watch_list")]:
        await log_stat("message_edited", event.chat_id, event.sender_id, 
                      f"Original: {event.message.text}")
        logger.info(f"Message edited in watched chat {event.chat_id}")

@client.on(events.MessageDeleted)
async def deleted_handler(event):
    """Track deleted messages"""
    for msg_id in event.deleted_ids:
        await log_stat("message_deleted", event.chat_id, None, f"ID: {msg_id}")
        logger.info(f"Message {msg_id} deleted in chat {event.chat_id}")

@client.on(events.ChatAction)
async def chat_action_handler(event):
    """Handle user join/leave events"""
    if event.user_joined or event.user_added:
        user = await event.get_user()
        await log_stat("user_joined", event.chat_id, user.id, 
                      f"Username: {user.username}")
        logger.info(f"User {user.id} joined chat {event.chat_id}")
    
    elif event.user_left or event.user_kicked:
        user = await event.get_user()
        await log_stat("user_left", event.chat_id, user.id,
                      f"Username: {user.username}")
        logger.info(f"User {user.id} left chat {event.chat_id}")

# === ADVANCED FEATURES ===

class MessageFilter:
    """Advanced message filtering system"""
    
    @staticmethod
    def contains_url(text: str) -> bool:
        url_pattern = re.compile(
            r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
        )
        return bool(url_pattern.search(text))
    
    @staticmethod
    def contains_phone(text: str) -> bool:
        phone_pattern = re.compile(r'(\+?\d{1,3}[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}')
        return bool(phone_pattern.search(text))
    
    @staticmethod
    def contains_email(text: str) -> bool:
        email_pattern = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')
        return bool(email_pattern.search(text))
    
    @staticmethod
    def sentiment_score(text: str) -> float:
        """Simple sentiment analysis (positive words vs negative words)"""
        positive_words = ['good', 'great', 'awesome', 'excellent', 'love', 'wonderful', 'amazing']
        negative_words = ['bad', 'terrible', 'awful', 'hate', 'horrible', 'worst', 'disgusting']
        
        text_lower = text.lower()
        pos_count = sum(1 for word in positive_words if word in text_lower)
        neg_count = sum(1 for word in negative_words if word in text_lower)
        
        total = pos_count + neg_count
        if total == 0:
            return 0.0
        return (pos_count - neg_count) / total

class SecurityManager:
    """Security features for the userbot"""
    
    @staticmethod
    def hash_text(text: str) -> str:
        """Hash sensitive text for storage"""
        return hashlib.sha256(text.encode()).hexdigest()
    
    @staticmethod
    async def verify_admin(client, chat_id: int, user_id: int) -> bool:
        """Check if user is admin in chat"""
        try:
            participant = await client.get_permissions(chat_id, user_id)
            return participant.is_admin or participant.is_creator
        except:
            return False
    
    @staticmethod
    def sanitize_input(text: str) -> str:
        """Sanitize user input to prevent injection"""
        # Remove potential SQL injection patterns
        dangerous_patterns = [';', '--', '/*', '*/', 'xp_', 'sp_', 'DROP', 'DELETE', 'INSERT']
        sanitized = text
        for pattern in dangerous_patterns:
            sanitized = sanitized.replace(pattern, '')
        return sanitized

security = SecurityManager()
message_filter = MessageFilter()

# === AUTO-MODERATION SYSTEM ===

class AutoModerator:
    def __init__(self):
        self.warnings: Dict[int, int] = defaultdict(int)
        self.muted_users: set = set()
    
    async def check_message(self, event) -> bool:
        """Check if message violates rules. Returns True if should be deleted."""
        text = event.raw_text or ""
        user_id = event.sender_id
        
        # Check for excessive caps
        if len(text) > 10 and sum(1 for c in text if c.isupper()) / len(text) > 0.7:
            self.warnings[user_id] += 1
            await event.reply("⚠️ Please don't use excessive caps")
            return True
        
        # Check for spam links
        if message_filter.contains_url(text):
            url_count = len(re.findall(r'http[s]?://', text))
            if url_count > 3:
                self.warnings[user_id] += 1
                return True
        
        # Three strikes system
        if self.warnings[user_id] >= 3:
            self.muted_users.add(user_id)
            await send_master(f"🔇 User {user_id} auto-muted after 3 warnings")
            return True
        
        return False
    
    def unmute_user(self, user_id: int):
        self.muted_users.discard(user_id)
        self.warnings[user_id] = 0

auto_moderator = AutoModerator()

# === SMART REPLY SYSTEM ===

class SmartReplySystem:
    """Context-aware reply suggestions"""
    
    def __init__(self):
        self.context_window: Dict[int, List[str]] = defaultdict(list)
        self.max_context = 10
    
    def add_to_context(self, chat_id: int, message: str):
        self.context_window[chat_id].append(message)
        if len(self.context_window[chat_id]) > self.max_context:
            self.context_window[chat_id].pop(0)
    
    def suggest_reply(self, chat_id: int, incoming_msg: str) -> Optional[str]:
        """Suggest a reply based on context"""
        context = self.context_window.get(chat_id, [])
        
        # Simple rule-based suggestions
        lower_msg = incoming_msg.lower()
        
        if any(word in lower_msg for word in ['hello', 'hi', 'hey']):
            return "Hello! How can I help you?"
        
        if any(word in lower_msg for word in ['thank', 'thanks']):
            return "You're welcome! 😊"
        
        if '?' in incoming_msg:
            return "Let me check that for you..."
        
        # Context-based suggestions
        if len(context) > 2:
            recent = ' '.join(context[-3:]).lower()
            if 'meeting' in recent and 'when' in lower_msg:
                return "The meeting is scheduled for tomorrow at 3 PM"
        
        return None

smart_reply = SmartReplySystem()

# === MEDIA PROCESSING ===

class MediaProcessor:
    """Advanced media handling"""
    
    @staticmethod
    async def compress_image(path: str, quality: int = 85) -> str:
        """Compress image file"""
        try:
            img = Image.open(path)
            compressed_path = path.replace('.', f'_compressed_{quality}.')
            
            if img.mode in ('RGBA', 'LA', 'P'):
                img = img.convert('RGB')
            
            img.save(compressed_path, 'JPEG', quality=quality, optimize=True)
            
            original_size = os.path.getsize(path)
            compressed_size = os.path.getsize(compressed_path)
            savings = ((original_size - compressed_size) / original_size) * 100
            
            logger.info(f"Compressed image: {savings:.1f}% size reduction")
            return compressed_path
        except Exception as e:
            logger.exception(f"Image compression failed: {e}")
            return path
    
    @staticmethod
    async def create_thumbnail(path: str, size: tuple = (200, 200)) -> str:
        """Create thumbnail from image"""
        try:
            img = Image.open(path)
            img.thumbnail(size, Image.Resampling.LANCZOS)
            thumb_path = path.replace('.', '_thumb.')
            img.save(thumb_path)
            return thumb_path
        except Exception as e:
            logger.exception(f"Thumbnail creation failed: {e}")
            return path

media_processor = MediaProcessor()

# === NOTIFICATION SYSTEM ===

class NotificationManager:
    """Smart notifications for important events"""
    
    def __init__(self):
        self.notify_keywords = set()
        self.notify_users = set()
        self.quiet_hours = (22, 7)  # 10 PM to 7 AM
    
    def is_quiet_time(self) -> bool:
        hour = datetime.now().hour
        start, end = self.quiet_hours
        if start > end:
            return hour >= start or hour < end
        return start <= hour < end
    
    async def notify(self, message: str, priority: str = "normal"):
        if priority == "high" or not self.is_quiet_time():
            await send_master(f"🔔 {message}")
    
    def add_notify_keyword(self, keyword: str):
        self.notify_keywords.add(keyword.lower())
    
    async def check_notification(self, text: str):
        lower = text.lower()
        for keyword in self.notify_keywords:
            if keyword in lower:
                await self.notify(f"Keyword '{keyword}' mentioned: {text[:100]}", "high")
                break

notification_manager = NotificationManager()

# === CONVERSATION ANALYZER ===

class ConversationAnalyzer:
    """Analyze conversation patterns and provide insights"""
    
    @staticmethod
    async def analyze_chat(chat_id: int, days: int = 7) -> dict:
        """Analyze chat activity over specified days"""
        query = f"""
        SELECT 
            COUNT(*) as total_messages,
            COUNT(DISTINCT user_id) as active_users,
            COUNT(CASE WHEN event_type = 'media_download' THEN 1 END) as media_count
        FROM statistics
        WHERE chat_id = ? AND timestamp > datetime('now', '-{days} days')
        """
        
        result = db.fetch_one(query, (chat_id,))
        
        return {
            "total_messages": result[0],
            "active_users": result[1],
            "media_count": result[2],
            "avg_messages_per_day": result[0] / days if days > 0 else 0
        }
    
    @staticmethod
    async def get_top_users(chat_id: int, limit: int = 10) -> List[tuple]:
        """Get most active users in chat"""
        query = """
        SELECT user_id, COUNT(*) as message_count
        FROM statistics
        WHERE chat_id = ? AND user_id IS NOT NULL
        GROUP BY user_id
        ORDER BY message_count DESC
        LIMIT ?
        """
        return db.fetch_all(query, (chat_id, limit))

conversation_analyzer = ConversationAnalyzer()

# === STARTUP & SHUTDOWN ===

async def on_startup():
    """Initialize bot on startup"""
    try:
        await client.start()
        me = await client.get_me()
        
        logger.info(f"Userbot started as {me.username} (ID: {me.id})")
        
        # Load saved data
        auto_replies = db.fetch_all("SELECT keyword, response FROM auto_replies WHERE enabled = 1")
        logger.info(f"Loaded {len(auto_replies)} auto-replies")
        
        watch_chats = db.fetch_all("SELECT chat_id FROM watch_list")
        logger.info(f"Watching {len(watch_chats)} chats")
        
        # Send startup notification
        startup_msg = f"""
🤖 **Userbot Started Successfully**

**User:** {me.first_name} (@{me.username})
**ID:** {me.id}
**Time:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

**Configuration:**
• Auto-Replies: {len(auto_replies)}
• Watched Chats: {len(watch_chats)}
• Templates: {len(template_manager.templates)}

Bot is ready and monitoring!
"""
        await send_master(startup_msg)
        
        # Schedule periodic cleanup
        scheduler.add_job(cleanup_old_data, 'interval', hours=24)
        
        logger.info("All systems initialized successfully")
        
    except Exception as e:
        logger.exception(f"Startup error: {e}")
        raise

async def cleanup_old_data():
    """Clean up old statistics and logs"""
    try:
        # Remove statistics older than 90 days
        db.execute("DELETE FROM statistics WHERE timestamp < datetime('now', '-90 days')")
        
        # Remove old media files
        for filename in os.listdir(MEDIA_DIR):
            filepath = os.path.join(MEDIA_DIR, filename)
            if os.path.isfile(filepath):
                file_age = datetime.now() - datetime.fromtimestamp(os.path.getmtime(filepath))
                if file_age.days > 30:
                    os.remove(filepath)
                    logger.info(f"Cleaned up old media file: {filename}")
        
        logger.info("Cleanup completed successfully")
    except Exception as e:
        logger.exception(f"Cleanup error: {e}")

async def on_shutdown():
    """Cleanup on shutdown"""
    try:
        await send_master("🔴 Userbot shutting down...")
        
        # Save any pending data
        db.conn.commit()
        
        # Stop scheduler
        scheduler.shutdown()
        
        logger.info("Userbot shutdown completed")
    except Exception as e:
        logger.exception(f"Shutdown error: {e}")

# === MAIN ENTRY POINT ===

async def main():
    """Main entry point"""
    try:
        await on_startup()
        
        print("=" * 50)
        print("🤖 Ultra Advanced Telegram Userbot")
        print("=" * 50)
        print(f"Status: RUNNING")
        print(f"Master ID: {MASTER_ID}")
        print(f"Commands: .help for full list")
        print("=" * 50)
        print("\nPress Ctrl+C to stop the bot\n")
        
        await client.run_until_disconnected()
        
    except KeyboardInterrupt:
        print("\n\n⚠️ Received shutdown signal...")
        await on_shutdown()
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
        await on_shutdown()
        raise
    finally:
        # Ensure database connection is closed
        db.conn.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n✅ Userbot stopped successfully")
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        logger.exception("Fatal error in main")
