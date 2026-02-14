# Minimal Discord bot (Python)

Slash command: **`/setup`** (admin-only) posts an embed + buttons in your setup channel.

## Setup

```bash
python -m venv .venv
```

### Windows (PowerShell)

```powershell
.venv\\Scripts\\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
# edit .env, then:
python bot.py
```

### macOS/Linux

```bash
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# edit .env, then:
python bot.py
```

## Notes

- Make sure your bot is added to your server.
- For fast command updates while developing, set `DISCORD_GUILD_ID` in `.env`.
- `SETUP_CHANNEL_ID` is now read from `config.yaml` (optional per server). If omitted, setup commands/buttons work in any channel (admin-only).
- Team emojis are resolved from server custom emojis by name (spaces become `_`).
- Tournament Results are posted to the channel in `RESULTS_TOURNAMENTS_CHANNEL_ID` (defaults to your `🏆・results`).
- Tournament host icons: put files in `tournament_icons/` (e.g. `tournament_icons/MRC.png`). In the modal, use `MRC | Tournament Name` so the bot can pick the right thumbnail.
- To enable `📅 Tournament Today`, set `NOTION_TOKEN` + `NOTION_DATABASE_ID` and (optionally) `UPCOMING_TOURNAMENTS_CHANNEL_ID`.
