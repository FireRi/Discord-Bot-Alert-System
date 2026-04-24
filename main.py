"""
=================================================================
  Curfew Alert — Discord Bot
  Subscribes to HiveMQ and sends a Discord message whenever
  a curfew violation is detected.
=================================================================

  INSTALL DEPENDENCIES
  ─────────────────────
  pip install -r requirements.txt

  SETUP — ENVIRONMENT VARIABLES
  ──────────────────────────────
  Never hardcode secrets. Set these environment variables instead:

  Local (terminal):
    set MQTT_PASS=your_hivemq_password        (Windows)
    export MQTT_PASS=your_hivemq_password     (Mac/Linux)

  On Railway:
    Go to your project → Variables tab → add each key/value there

  Required variables:
    MQTT_PASS      = your HiveMQ password
    DISCORD_TOKEN  = your Discord bot token
    CHANNEL_ID     = your Discord channel ID (integer)

  DISCORD BOT SETUP
  ─────────────────
  1. Go to https://discord.com/developers/applications
  2. Click "New Application" → give it a name
  3. Go to Bot → click "Add Bot" → copy the TOKEN
  4. Go to OAuth2 → URL Generator:
       Scopes     : bot
       Permissions: Send Messages, Embed Links, View Channels,
                    Mention Everyone
  5. Open the generated URL in your browser to invite the bot
  6. In Discord, right-click your channel → Copy Channel ID
     (Enable Developer Mode: Settings → Advanced → Developer Mode)

  RUN
  ───
  python discord_bot.py
=================================================================
"""

import paho.mqtt.client as mqtt
import discord
import asyncio
import json
import ssl
import threading
import os
from datetime import datetime

# ─── CONFIGURATION (loaded from environment variables) ────────
MQTT_HOST     = "64a5d6b81df04a718d5c868009f39096.s1.eu.hivemq.cloud"
MQTT_PORT     = 8883
MQTT_USER     = "DiscordBot"
MQTT_PASS     = os.environ["MQTT_PASS"]           # set in Railway → Variables
TOPIC         = "home/door/curfew_alert"

DISCORD_TOKEN = os.environ["DISCORD_TOKEN"]       # set in Railway → Variables
CHANNEL_ID    = int(os.environ["CHANNEL_ID"])     # set in Railway → Variables
# ─────────────────────────────────────────────────────────────

# Shared queue between MQTT thread and Discord async loop
alert_queue = asyncio.Queue()

# ─── DISCORD BOT ─────────────────────────────────────────────

intents = discord.Intents.default()
client  = discord.Client(intents=intents)


@client.event
async def on_ready():
    print(f"[Discord] Logged in as {client.user}")
    print(f"[Discord] Sending alerts to channel ID: {CHANNEL_ID}")
    client.loop.create_task(process_alert_queue())


async def process_alert_queue():
    """Continuously reads from the queue and sends Discord messages."""
    await client.wait_until_ready()
    channel = client.get_channel(CHANNEL_ID)

    if channel is None:
        print(f"[Discord] ERROR: Channel {CHANNEL_ID} not found! Check your CHANNEL_ID.")
        return

    while not client.is_closed():
        try:
            payload = await asyncio.wait_for(alert_queue.get(), timeout=1.0)
            embed   = build_embed(payload)
            await channel.send("@everyone 🚨 Curfew violation detected!")
            await channel.send(embed=embed)
            print(f"[Discord] Alert sent to #{channel.name}")
        except asyncio.TimeoutError:
            continue
        except Exception as e:
            print(f"[Discord] Error sending message: {e}")


def build_embed(payload: dict) -> discord.Embed:
    """Build a rich Discord embed from the MQTT payload."""
    timestamp   = payload.get("timestamp", "Unknown")
    date_str    = payload.get("date",      "Unknown")
    time_str    = payload.get("time",      "Unknown")
    device      = payload.get("device",    "Unknown")
    event       = payload.get("event",     "CURFEW_VIOLATION")
    day_of_week = ""

    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        day_of_week = dt.strftime("%A")   # e.g. "Thursday"
    except Exception:
        day_of_week = ""

    embed = discord.Embed(
        title       = "🚨 Curfew Violation Detected!",
        description = "The door was opened during restricted hours.",
        color       = discord.Color.red(),
        timestamp   = datetime.utcnow()
    )
    embed.add_field(name="📅 Date",   value=f"{day_of_week}, {date_str}", inline=True)
    embed.add_field(name="🕐 Time",   value=time_str,                     inline=True)
    embed.add_field(name="📟 Device", value=device,                       inline=True)
    embed.add_field(name="⚠️ Event",  value=event,                        inline=False)
    embed.set_footer(text=f"Timestamp: {timestamp}")

    return embed


# ─── MQTT LISTENER (runs in a background thread) ──────────────

def on_connect(mqtt_client, userdata, flags, rc):
    if rc == 0:
        print(f"[MQTT] Connected to {MQTT_HOST}")
        mqtt_client.subscribe(TOPIC)
        print(f"[MQTT] Subscribed to: {TOPIC}")
    else:
        print(f"[MQTT] Connection failed. Code: {rc}")


def on_message(mqtt_client, userdata, msg):
    print(f"\n[MQTT] Message received on '{msg.topic}'")
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
        print(f"[MQTT] Payload: {payload}")
        asyncio.run_coroutine_threadsafe(alert_queue.put(payload), client.loop)
    except json.JSONDecodeError:
        print(f"[MQTT] Could not parse message: {msg.payload}")


def start_mqtt():
    mqtt_client = mqtt.Client(client_id="DiscordBotMQTT", protocol=mqtt.MQTTv311)
    mqtt_client.username_pw_set(MQTT_USER, MQTT_PASS)
    mqtt_client.tls_set(cert_reqs=ssl.CERT_NONE)
    mqtt_client.tls_insecure_set(True)
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    mqtt_client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    mqtt_client.loop_forever()


# ─── MAIN ────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=== Curfew Alert Discord Bot ===\n")

    # Run MQTT in a background thread
    mqtt_thread = threading.Thread(target=start_mqtt, daemon=True)
    mqtt_thread.start()

    # Run Discord bot in the main thread (manages its own event loop)
    client.run(DISCORD_TOKEN)
