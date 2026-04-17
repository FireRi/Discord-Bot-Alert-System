"""
=================================================================
  ESP8266 Test Monitor — Discord Bot
  Subscribes to HiveMQ (home/door/test) and forwards every
  plain-text test message from the ESP8266 to a Discord channel.
=================================================================

  INSTALL DEPENDENCIES
  ─────────────────────
  pip install -r requirements.txt

  SETUP — ENVIRONMENT VARIABLES
  ──────────────────────────────
  Never hardcode secrets. Set these environment variables instead:

  Local (.env or terminal):
    MQTT_PASS      = your HiveMQ password
    DISCORD_TOKEN  = your Discord bot token
    CHANNEL_ID     = your Discord channel ID (integer)

  On Railway:
    Go to your project → Variables tab → add each key/value there

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
  python discord_bot_esp8266_test.py
=================================================================
"""

import paho.mqtt.client as mqtt
import discord
import asyncio
import ssl
import threading
import os
from datetime import datetime

# ─── CONFIGURATION (loaded from environment variables) ────────
MQTT_HOST     = "64a5d6b81df04a718d5c868009f39096.s1.eu.hivemq.cloud"
MQTT_PORT     = 8883
MQTT_USER     = "DiscordBot"
MQTT_PASS     = os.environ["MQTT_PASS"]           # set in Railway → Variables
TEST_TOPIC    = "home/door/test"

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
    print(f"[Discord] Forwarding ESP8266 test messages to channel ID: {CHANNEL_ID}")
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
            raw_message = await asyncio.wait_for(alert_queue.get(), timeout=1.0)
            embed = build_embed(raw_message)
            await channel.send("@everyone 📡 New message from ESP8266!")
            await channel.send(embed=embed)
            print(f"[Discord] Message forwarded to #{channel.name}")
        except asyncio.TimeoutError:
            continue
        except Exception as e:
            print(f"[Discord] Error sending message: {e}")


def build_embed(raw_message: str) -> discord.Embed:
    """Build a Discord embed from a plain-text ESP8266 test message."""
    now = datetime.utcnow()

    embed = discord.Embed(
        title       = "📡 ESP8266 Test Message",
        description = f"```{raw_message}```",
        color       = discord.Color.green(),
        timestamp   = now
    )
    embed.add_field(name="📶 Topic",    value=f"`{TEST_TOPIC}`",    inline=True)
    embed.add_field(name="🕐 Received", value=now.strftime("%H:%M:%S UTC"), inline=True)
    embed.set_footer(text="ESP8266 MQTT Test Monitor")

    return embed


# ─── MQTT LISTENER (runs in a background thread) ──────────────

def on_connect(mqtt_client, userdata, flags, rc):
    if rc == 0:
        print(f"[MQTT] Connected to {MQTT_HOST}")
        mqtt_client.subscribe(TEST_TOPIC)
        print(f"[MQTT] Subscribed to: {TEST_TOPIC}")
    else:
        print(f"[MQTT] Connection failed. Code: {rc}")


def on_message(mqtt_client, userdata, msg):
    raw = msg.payload.decode("utf-8")
    print(f"\n[MQTT] Received on '{msg.topic}': {raw}")
    # Thread-safely push plain text to the asyncio queue
    asyncio.run_coroutine_threadsafe(alert_queue.put(raw), client.loop)


def start_mqtt():
    mqtt_client = mqtt.Client(client_id="DiscordBotMQTT_Test", protocol=mqtt.MQTTv311)
    mqtt_client.username_pw_set(MQTT_USER, MQTT_PASS)
    mqtt_client.tls_set(cert_reqs=ssl.CERT_NONE)
    mqtt_client.tls_insecure_set(True)
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    mqtt_client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    mqtt_client.loop_forever()


# ─── MAIN ────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=== ESP8266 Test Monitor Discord Bot ===\n")

    # Run MQTT in a background thread
    mqtt_thread = threading.Thread(target=start_mqtt, daemon=True)
    mqtt_thread.start()

    # Run Discord bot in the main thread (manages its own event loop)
    client.run(DISCORD_TOKEN)
