🚀 Diskwala Bot

«⚡ A powerful Telegram bot for downloading and streaming videos from Diskwala / Flezen links directly through Telegram.»

---

✨ Features

- 📥 Direct Downloads — Download Diskwala/Flezen videos directly to Telegram
- ▶️ Stream Links — Get streamable links for in-app viewing
- 🔗 Multi-Link Support — Send and process multiple links at once
- 📊 Progress Tracking — Real-time download progress with speed display
- 💎 Premium System — Premium plans with daily free-download limits
- 🗑️ Auto Delete — Automatically delete sent files after a configurable time
- 🛡️ Admin Tools — Premium management, ban/unban, statistics and broadcasting
- 💾 Backup Channels — Automatically copy successfully downloaded videos to linked channels/groups
- 📝 Event Logging — Monitor bot startup, new users and successful downloads
- 🗄️ MongoDB Storage — Persistent file cache and user management

---

🧰 Prerequisites

Before starting, make sure you have:

- 🐍 Python 3.10+
- 🤖 Telegram Bot Token — Get it from "@BotFather"
- 🔑 Telegram API Credentials — Get them from "my.telegram.org"
- 🗄️ MongoDB Database
- 📱 Telethon Session String

---

⚙️ Setup

1️⃣ Get Telegram Bot Token

1. Open Telegram and search for "@BotFather" (https://t.me/BotFather)
2. Send "/newbot"
3. Follow the instructions provided by BotFather
4. Copy your generated Bot Token

---

2️⃣ Get Telegram API Credentials

1. Visit "my.telegram.org" (https://my.telegram.org)
2. Log in using your Telegram phone number
3. Open API development tools
4. Create a new application
5. Copy your:

api_id
api_hash

---

3️⃣ Generate Telethon Session String

Create a file named:

gen_session.py

Add:

from telethon.sync import TelegramClient
from telethon.sessions import StringSession

api_id = 12345678          # your api_id
api_hash = "your_api_hash"

with TelegramClient(StringSession(), api_id, api_hash) as client:
    print(client.session.save())

Run:

python gen_session.py

🔐 Log in when prompted and copy the generated session string.

«⚠️ SECURITY WARNING:
Your session string is equivalent to access to your Telegram account.
Never share it publicly or commit it to GitHub.»

---

4️⃣ Install & Run

Clone/download the project and enter the directory:

cd diskwala_bot

Install dependencies:

pip install -r requirements.txt

Create your environment file:

cp .env.example .env

Edit ".env" and add your credentials.

Finally, start the bot:

python main.py

---

🔐 Environment Variables

Configure your ".env" file:

API_ID=your_api_id
API_HASH=your_api_hash
BOT_TOKEN=your_bot_token
SESSION=your_session_string
OWNER_ID=your_telegram_user_id
MONGO_URI=your_mongodb_connection_string

⚡ Optional Configuration

ADMINS=123456789,987654321
DAILY_FREE_LIMIT=10
AUTO_DELETE_SECONDS=3600
LOG_CHANNEL=-1001234567890

Variable| Description
"API_ID"| Telegram API ID
"API_HASH"| Telegram API Hash
"BOT_TOKEN"| Telegram Bot Token
"SESSION"| Telethon user session string
"OWNER_ID"| Main bot owner Telegram ID
"MONGO_URI"| MongoDB connection string
"ADMINS"| Additional admin Telegram IDs
"DAILY_FREE_LIMIT"| Daily downloads allowed for free users
"AUTO_DELETE_SECONDS"| Auto-delete delay in seconds
"LOG_CHANNEL"| Channel/group ID for event logs

---

🤖 Usage

1. 🚀 Start the Bot

Open your bot on Telegram and send:

/start

2. 🔗 Send a Diskwala Link

Example:

https://www.diskwala.com/app/xxxxx

3. 🎯 Choose an Action

The bot will provide options such as:

📥 Download File
▶️ Get Stream Link

---

🧾 Commands

👤 User Commands

Command| Description
"/start"| 🚀 Welcome message
"/help"| 📚 Show usage instructions
"/myplan"| 📊 Show your plan / account status

"/myplan" is also available through the 📊 My Status button.

---

👑 Admin Commands

«🔒 The following commands require the user to be listed in "ADMINS" or be the configured "OWNER_ID".»

/addpremium <user_id> <days|lifetime>

💎 Grant premium access.

/removepremium <user_id>

❌ Revoke premium access.

/ban <user_id>

🚫 Ban a user.

/unban <user_id>

✅ Unban a user.

/stats

📊 View bot usage statistics.

/broadcast <message>

📢 Broadcast a message to all known users.

You can also reply to an existing Telegram message with:

/broadcast

/set_channel_id <-100xxxxxxxxxx>

💾 Link a channel/group as a backup destination.

/channel_id

📋 List all linked backup channels/groups.

/del_channel_id [id]

🗑️ Unlink a specific channel, or all channels if no ID is provided.

---

💾 Linked Backup Channels

Admins can connect one or more Telegram channels/groups using:

/set_channel_id <channel_id>

Every successfully downloaded video will also be copied to the linked destination.

⭐ Why use Backup Channels?

- 🗄️ Maintain a persistent video archive
- 🔄 Keep files beyond the auto-delete period
- 📦 Store downloaded content separately
- 🛡️ Reduce the risk of losing sent files

🔎 Getting a Channel ID

Forward any message from the channel/group to:

@MissRose_bot

The bot must have administrator permissions in the destination channel/group.

---

📝 Log Channel

Set "LOG_CHANNEL" to a single Telegram channel/group ID to receive bot event logs.

LOG_CHANNEL=-1001234567890

Set it to "0" or leave it unset to disable logging.

📌 Logged Events

- 🚀 Bot startup notification
- 👤 First "/start" notification for each new user
- 📥 Successful download notifications
- 🔗 Downloaded link information

«ℹ️ Important: "LOG_CHANNEL" is separate from the "/set_channel_id" backup channels.

📝 "LOG_CHANNEL" → Text/event logs
💾 Backup channels → Actual downloaded video files»

---

💎 Premium Plans

Free users receive:

DAILY_FREE_LIMIT=10

downloads per day by default.

🔄 The free-download counter resets at UTC midnight.

The 💎 Plans menu displays available pricing tiers and allows users to select a plan.

«⚠️ There is currently no payment gateway integrated into the bot.»

Users should contact an administrator for premium activation.

The admin can then run:

/addpremium <user_id> <days|lifetime>

💎 Premium Options

/addpremium 123456789 7

⏳ Premium for 7 days.

/addpremium 123456789 30

📅 Premium for 30 days.

/addpremium 123456789 lifetime

♾️ Lifetime premium.

---

🗑️ Auto-Delete System

The bot can automatically remove sent videos after a configurable period.

Default:

AUTO_DELETE_SECONDS=3600

⏱️ "3600" seconds = 1 hour

Set:

AUTO_DELETE_SECONDS=0

to disable auto-delete.

📌 How It Works

1. 📥 Video is downloaded
2. 📤 Video is sent to the user
3. ⏳ Auto-delete timer starts
4. 🗑️ File is automatically deleted after the configured duration
5. 🔔 A notice tells the user to re-download or forward the file if they want to keep it

Every sent file also includes an upfront auto-delete notice in its caption.

---

🗂️ File Structure

diskwala_bot/
│
├── main.py
│   └── Telegram bot handlers, plans, admin commands & auto-delete
│
├── diskwala.py
│   └── Diskwala API extraction logic
│
├── db.py
│   └── MongoDB file cache, premium, ban & daily-limit tracking
│
├── config.py
│   └── Environment/configuration management
│
├── requirements.txt
│   └── Python dependencies
│
├── render.yaml
│   └── Render.com deployment configuration
│
├── Dockerfile
│   └── Docker deployment configuration
│
├── .env.example
│   └── Environment variable template
│
└── README.md
    └── Project documentation

---

🧠 Technical Notes

- 🔐 The bot requires a Telethon user session to authenticate with Diskwala's API.
- ⚠️ The Telethon session string should be treated like a password.
- 🚫 Never publish your ".env" file or session string.
- 📥 Videos are temporarily downloaded before being sent.
- 🗑️ Temporary video files are deleted after sending.
- 💾 MongoDB is used for persistent cache and user-related data.
- ☁️ The project includes configuration files for containerized/Render deployment.

---

🚀 Deployment

The project includes:

🐳 Dockerfile
☁️ render.yaml

This makes the bot suitable for deployment on supported container/cloud hosting platforms.

Before deploying, make sure all required environment variables are configured securely.

---

🛡️ Security Checklist

Before making the bot public:

✅ Keep BOT_TOKEN private
✅ Keep API_HASH private
✅ Keep SESSION private
✅ Keep MONGO_URI private
✅ Never upload .env to GitHub
✅ Add .env to .gitignore
✅ Restrict admin commands
✅ Give the bot only required Telegram permissions

---

📌 Important

«🔐 Never share your Telegram session string, bot token, API credentials, or MongoDB credentials with anyone.»

«💡 For production deployments, always use environment variables or a secure secrets manager instead of hardcoding credentials.»

---

⚡ Quick Start

git clone <your-repository>
cd diskwala_bot

pip install -r requirements.txt

cp .env.example .env
nano .env

python main.py

Then open your Telegram bot and send:

/start

---

👨‍💻 Project

Diskwala Bot — A feature-rich Telegram automation project focused on Diskwala/Flezen video downloading, streaming, premium management, logging and automated file handling.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━

⚡ Fast • Secure • Reliable • Developer Friendly

❤️ Powered by Anuj Kumar .
