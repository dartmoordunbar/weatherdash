# WeatherDash

A live dashboard and recorder for a Davis WeatherLink Live (or WeatherLink Console) on your home network.

- Reads the station over your LAN every 10 seconds and saves every reading to a local SQLite file (`weather.db`).
- Shows live wind and rain every 2.5 seconds, using the station's UDP broadcast.
- Pushes updates straight to any browser on your network. There's no refresh button.
- Shows history for 6 hours, 24 hours, 7 days, 30 days or 1 year: temperature (with min/max band), wind and gusts, pressure, rain and humidity.
- **Fills in gaps.** If the station drops off the network, or this program wasn't running, the dashboard spots the hole in the record. When the gap is over, it fetches the missing readings from WeatherLink.com. On the chart, those readings are drawn as a dashed blue line.
- Lets you export everything to CSV.

Needs only Python 3.8 or newer. There's nothing to install.

## 1. Find your station's address

Open the WeatherLink app and go to Settings › Device Info, or look in your router's list of devices. You're looking for something like `192.168.1.47`. Check it works by opening `http://192.168.1.47/v1/current_conditions` in a browser.

Tip: give the station a fixed (reserved) IP address in your router, so the address doesn't change.

## 2. Run it

```bash
python3 server.py --host 192.168.1.47 --name "Home Station"
```

Then open `http://<this computer's IP>:8080` on any phone or computer on the same network.

Run it on something that stays on, like a Raspberry Pi, NAS or home server. While it isn't running, nothing is recorded on your LAN. Gaps can only be filled from the cloud (step 3).

## 3. Turn on backfill (optional)

The station keeps no history you can read over the LAN. **Missed readings can only come back from WeatherLink.com**, through its v2 API. Davis only gives historic data to stations with a **Pro or Pro+** subscription. On the free Basic plan, the dashboard still marks gaps, but it can't fill them.

1. Sign in at weatherlink.com, open **Account**, and click **Generate v2 Key**.
2. Start the program with the key and secret:

```bash
WL_API_KEY=xxxxx WL_API_SECRET=yyyyy python3 server.py --host 192.168.1.47
```

It finds gaps longer than 2 minutes in the last 30 days, and checks for new ones every 5 minutes. If an outage runs past 15 minutes, it also starts pulling the cloud's readings, so the charts keep moving while the station is offline. Each gap is filled once. A failed attempt is retried after an hour. You can see what happened to each gap at `/api/gaps`.

Rain isn't counted twice. Once the cloud's readings for a gap are in, the dashboard takes them off the catch-up total in the first reading after the gap.

## 4. Start on boot (Raspberry Pi or Linux)

Edit `weatherdash.service`: fill in your folder, your station's address and (optionally) your keys. Then:

```bash
sudo cp weatherdash.service /etc/systemd/system/
sudo systemctl enable --now weatherdash
journalctl -u weatherdash -f      # watch the log
```

## Or run it in Docker

Every push to `main` builds an image for amd64, arm64 and armv7 (Raspberry Pi), and publishes it as `ghcr.io/dartmoordunbar/weatherdash:latest`. Tags like `v1.2.0` also publish `1.2.0` and `1.2`.

Edit the station address in `docker-compose.yml`, then:

```bash
docker compose up -d
```

The compose file uses **host networking**, because the station's live-wind broadcast is UDP and doesn't reach a container on Docker's default bridge network. Readings are kept in `./data/weather.db`. Set `TZ` so "today" and the daily rain totals reset at your local midnight.

Plain `docker run` works too:

```bash
docker run -d --name weatherdash --restart unless-stopped --network host \
  -e WLL_HOST=192.168.1.47 -e TZ=Europe/London -v "$PWD/data:/data" \
  ghcr.io/dartmoordunbar/weatherdash:latest
```

## Settings

| Flag | Env var | Default | |
|---|---|---|---|
| `--host` | `WLL_HOST` | (required) | The station's IP or hostname |
| `--name` | `STATION_NAME` | Home Station | Title shown on the dashboard |
| `--port` | `PORT` | 8080 | Web port |
| `--db` | `DB_PATH` | `weather.db` next to the script | Where readings are stored |
| `--poll` | `POLL_SECONDS` | 10 | Seconds between readings (Davis allows 10 at the fastest) |
| `--txid` | `ISS_TXID` | first one found | Which ISS to use, if you have more than one |
| `--gap-seconds` | `GAP_SECONDS` | 120 | How long a silence must be to count as a gap |
| `--backfill-days` | `BACKFILL_DAYS` | 30 | How far back to look for gaps |
| `--api-key`, `--api-secret` | `WL_API_KEY`, `WL_API_SECRET` | none | WeatherLink.com v2 keys |
| `--station-id` | `WL_STATION_ID` | first station on the account | Which cloud station to use |

## Good to know

- **Storage:** a reading every 10 seconds adds up to about 3 million rows a year, roughly 150–250 MB. To keep the file, back up `weather.db`.
- **Firewall:** live wind uses UDP port 22222. If the compass only updates every 10 seconds, open that port on the computer running WeatherDash.
- **Security:** there's no login. Keep it on your home network and don't forward the port to the internet.
- **Endpoints:** `/api/now`, `/api/history?range=24h`, `/api/events` (live stream), `/api/gaps`, `/api/export.csv?days=30`.
- **Cloud field names:** the dashboard reads WeatherLink.com's usual archive fields (`temp_avg`, `wind_speed_hi`, `rainfall_mm`, `bar_sea_level`…). If a backfilled reading is missing a value, run with `-v` and send me the log.
