import csv
import re
from collections import Counter
from datetime import datetime, timezone
import random
from pathlib import Path
from zoneinfo import ZoneInfo

import discord
import httpx
import yaml

from . import config
from .academy import ROLES, TEAMS_YAML_PATH, create_teams_from_file, register_player, unregister_player
from .team_emojis import emoji_for, emoji_for_org, emoji_name_for_team, _find_custom_emoji
from .team_icons import find_team_icon
from .tournament_icons import find_icon
from .notion_api import NotionClient
from .todays_tournaments import (
    cet_day,
    discord_timestamp,
    detect_props,
    extract_tournament,
    notion_query_payload_for_today_cups,
    today_cet,
)


_TS_RE = re.compile(r"<t:(\d+)(?::[tTdDfFR])?>")
_CET = ZoneInfo("Europe/Paris")
_USER_MENTION_RE = re.compile(r"<@!?(\d+)>")
_FLAG_ALIAS_RE = re.compile(r"^:flag_([a-z]{2}):$", re.IGNORECASE)
_MESSAGE_LINK_RE = re.compile(r"https?://(?:canary\.)?discord(?:app)?\.com/channels/\d+/(\d+)/(\d+)")
_FIRST_INT_RE = re.compile(r"\d+")

_REPO_ROOT = Path(__file__).resolve().parents[1]
_LEADERBOARD_CSV = _REPO_ROOT / "leaderboard" / "output" / "leaderboard_aggregated.csv"
_PART_LEADERBOARD_PRT_DIR = _REPO_ROOT / "leaderboard" / "csv_prt"
_ROSTERS_YAML = _REPO_ROOT / "leaderboard" / "output" / "rosters.yaml"
_PREDICTIONS_CSV = _REPO_ROOT / "leaderboard" / "output" / "predictions.csv"
_PREDICTION_FIELDNAMES = ("poll_date", "message_id", "question", "winning_answer", "all_people", "right_people")

_RULEBOOK_URL = "https://www.notion.so/Rulebook-2cd037d9654180bdba21ea03e737d8d8?source=copy_link"
_FRT_RULES_URL = "https://discord.com/channels/1451978161318527068/1454676450631356550"
_LEADERBOARD_POINT_RANGES: list[tuple[int, int, int]] = [
    (1, 1, 100),
    (2, 2, 80),
    (3, 3, 65),
    (4, 4, 55),
    (5, 6, 45),
    (7, 8, 35),
    (9, 12, 25),
    (13, 16, 18),
    (17, 24, 12),
    (25, 32, 8),
    (33, 48, 4),
    (49, 64, 1),
]


async def _get_sendable_channel(
    guild: discord.Guild,
    channel_id: int,
) -> discord.abc.Messageable | None:
    ch = guild.get_channel(channel_id)
    if ch is None:
        try:
            ch = await guild.fetch_channel(channel_id)
        except discord.DiscordException:
            ch = None
    return ch if (ch is not None and hasattr(ch, "send")) else None


async def _delete_messages_best_effort(
    channel: discord.abc.Messageable,
    message_ids: list[int],
) -> None:
    for mid in message_ids:
        try:
            msg = await channel.fetch_message(mid)  # type: ignore[attr-defined]
            await msg.delete()
        except Exception:
            pass


def _truncate_text(text: str, limit: int) -> str:
    normalized = " ".join((text or "").split())
    if len(normalized) <= limit:
        return normalized
    return normalized[: max(0, limit - 1)].rstrip() + "…"


def _poll_media_text(media: object) -> str:
    if media is None:
        return ""

    if isinstance(media, str):
        return " ".join(media.split())

    text = getattr(media, "text", None)
    if text:
        return " ".join(str(text).split())

    if isinstance(media, dict):
        raw_text = media.get("text")
        if raw_text:
            return " ".join(str(raw_text).split())

    # Some poll payloads can come back in shapes where `text` is empty but the
    # object still stringifies to the question content.
    rendered = " ".join(str(media).split())
    if rendered and "object at 0x" not in rendered:
        return rendered

    return ""


def _poll_question_text(poll: discord.Poll) -> str:
    direct = " ".join(str(getattr(poll, "question", "") or "").split())
    if direct:
        return direct

    fallback = _poll_media_text(getattr(poll, "_question_media", None))
    if fallback:
        return fallback

    return ""


def _prediction_date_for_message(message: discord.Message) -> str:
    return message.created_at.astimezone(_CET).strftime("%Y-%m-%d")


def _format_prediction_people(voters: list[discord.abc.User]) -> str:
    entries: list[str] = []
    seen: set[int] = set()
    for voter in voters:
        if voter.id in seen:
            continue
        seen.add(voter.id)
        entries.append(str(voter.id))
    return "; ".join(sorted(entries))


async def _collect_poll_voters(poll: discord.Poll) -> list[discord.abc.User]:
    voters: list[discord.abc.User] = []
    seen: set[int] = set()

    for answer in poll.answers:
        async for voter in answer.voters(limit=None):
            if voter.id in seen:
                continue
            seen.add(voter.id)
            voters.append(voter)

    return voters


def _load_prediction_rows() -> list[dict[str, str]]:
    if not _PREDICTIONS_CSV.exists():
        return []

    with _PREDICTIONS_CSV.open("r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            raise ValueError("Predictions CSV has no header row.")

        missing = [name for name in _PREDICTION_FIELDNAMES if name not in set(reader.fieldnames)]
        if missing:
            raise ValueError(f"Predictions CSV is missing columns: {', '.join(missing)}")

        return list(reader)


def _get_prediction_row(message_id: int) -> dict[str, str] | None:
    wanted = str(int(message_id))
    for row in _load_prediction_rows():
        if (row.get("message_id") or "").strip() == wanted:
            return row
    return None


def _append_prediction_row(row: dict[str, str]) -> None:
    _PREDICTIONS_CSV.parent.mkdir(parents=True, exist_ok=True)
    file_exists = _PREDICTIONS_CSV.exists()
    with _PREDICTIONS_CSV.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(_PREDICTION_FIELDNAMES))
        if not file_exists:
            writer.writeheader()
        writer.writerow({name: row.get(name, "") for name in _PREDICTION_FIELDNAMES})


def _parse_prediction_people(raw_value: str) -> list[str]:
    values = [part.strip() for part in (raw_value or "").split(";")]
    return [value for value in values if value and value.isdigit()]


def _parse_prediction_month(raw_value: str) -> tuple[int, int]:
    raw = (raw_value or "").strip()
    if not raw:
        raise ValueError("Enter a month/year like `03/2026` or `2026-03`.")

    for fmt in ("%m/%Y", "%Y-%m", "%Y/%m", "%m-%Y"):
        try:
            parsed = datetime.strptime(raw, fmt)
            return parsed.year, parsed.month
        except ValueError:
            continue

    raise ValueError("Invalid month/year. Use `03/2026` or `2026-03`.")


def _prediction_month_label(year: int, month: int) -> str:
    return datetime(year=year, month=month, day=1).strftime("%B %Y")


def _hall_of_fame_channel_id(
    server: config.ServerConfig,
    *,
    tournament_type: str | None = None,
) -> int | None:
    value = server.hall_of_fame_channel_id
    if isinstance(value, int):
        return int(value)
    if isinstance(value, dict) and tournament_type:
        return value.get(tournament_type.strip().upper())
    return None


def _leaderboard_channel_id(
    server: config.ServerConfig,
    *,
    tournament_type: str | None = None,
) -> int | None:
    value = server.leaderboard_channel_id
    if isinstance(value, int):
        return int(value)
    if isinstance(value, dict) and tournament_type:
        return value.get(tournament_type.strip().upper())
    return None


def _build_prediction_results_embed(
    *,
    year: int,
    month: int,
    results: list[tuple[str, int, int]],
    polls_count: int,
) -> discord.Embed:
    label = _prediction_month_label(year, month)
    embed = discord.Embed(
        title=f"🔮 Predictor of the Month — {label}",
        color=0xBE629B,
    )

    if not results:
        embed.description = "No prediction participation found for that month."
        return embed

    user_vals = [
        f"<@{user_id}>"
        for user_id, _correct, _total in results[:10]
    ]
    correct_vals = [
        str(correct)
        for _user_id, correct, _total in results[:10]
    ]
    accuracy_vals = [
        f"{(correct / total) * 100:.1f}%"
        for _user_id, correct, total in results[:10]
    ]

    embed.add_field(name="User", value="\n".join(user_vals) or "-", inline=True)
    embed.add_field(name="Correct", value="\n".join(correct_vals) or "-", inline=True)
    embed.add_field(name="Accuracy", value="\n".join(accuracy_vals) or "-", inline=True)
    embed.set_footer(text=f"{polls_count} tracked poll(s) in {label}")
    return embed


def _parse_message_locator(raw_value: str) -> tuple[int, int | None]:
    raw = (raw_value or "").strip()
    if not raw:
        raise ValueError("Enter a poll message ID.")

    link_match = _MESSAGE_LINK_RE.search(raw)
    if link_match:
        channel_id = int(link_match.group(1))
        message_id = int(link_match.group(2))
        return message_id, channel_id

    if raw.isdigit():
        return int(raw), None

    raise ValueError("Enter a numeric Discord message ID or a full Discord message link.")


async def _find_message_by_id(
    guild: discord.Guild,
    message_id: int,
    *,
    channel_id: int | None = None,
    client: discord.Client | None = None,
) -> discord.Message | None:
    if channel_id is not None:
        channel = guild.get_channel_or_thread(channel_id)
        if channel is None:
            try:
                channel = await guild.fetch_channel(channel_id)
            except discord.DiscordException:
                channel = None
        if channel is not None and hasattr(channel, "fetch_message"):
            try:
                return await channel.fetch_message(message_id)  # type: ignore[attr-defined]
            except discord.DiscordException:
                return None

    if client is not None:
        cached = discord.utils.get(getattr(client, "cached_messages", []), id=message_id)
        if cached is not None and cached.guild and cached.guild.id == guild.id:
            return cached

    around = discord.utils.snowflake_time(message_id)
    seen_channel_ids: set[int] = set()
    searchable_channels: list[discord.abc.Messageable] = []

    for channel in guild.text_channels:
        if channel.id in seen_channel_ids:
            continue
        seen_channel_ids.add(channel.id)
        searchable_channels.append(channel)

    for thread in guild.threads:
        if thread.id in seen_channel_ids:
            continue
        seen_channel_ids.add(thread.id)
        searchable_channels.append(thread)

    try:
        fetched_channels = await guild.fetch_channels()
    except discord.DiscordException:
        fetched_channels = []

    for channel in fetched_channels:
        if not isinstance(channel, discord.TextChannel):
            continue
        if channel.id in seen_channel_ids:
            continue
        seen_channel_ids.add(channel.id)
        searchable_channels.append(channel)

    for channel in searchable_channels:
        history = getattr(channel, "history", None)
        if not callable(history):
            continue
        try:
            async for message in channel.history(limit=100, around=around):  # type: ignore[attr-defined]
                if message.id == message_id:
                    return message
        except (discord.Forbidden, discord.HTTPException, TypeError):
            continue

    return None


class ConfirmPostView(discord.ui.View):
    def __init__(
        self,
        *,
        requester_id: int,
        test_channel_id: int,
        preview_message_ids: list[int],
        publish_fn,
    ):
        super().__init__(timeout=300)
        self.requester_id = requester_id
        self.test_channel_id = int(test_channel_id)
        self.preview_message_ids = list(preview_message_ids)
        self.publish_fn = publish_fn

    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.success)
    async def confirm(self, interaction: discord.Interaction, _: discord.ui.Button):
        if interaction.user.id != self.requester_id:
            await interaction.response.send_message("Only the admin who opened this can confirm.", ephemeral=True)
            return
        if not interaction.guild:
            await interaction.response.send_message("Run this in the server.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        test_ch = await _get_sendable_channel(interaction.guild, self.test_channel_id)
        if test_ch is not None and self.preview_message_ids:
            await _delete_messages_best_effort(test_ch, self.preview_message_ids)

        result = await self.publish_fn(interaction)
        await interaction.followup.send(result or "Confirmed and posted.", ephemeral=True)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, _: discord.ui.Button):
        if interaction.user.id != self.requester_id:
            await interaction.response.send_message("Only the admin who opened this can cancel.", ephemeral=True)
            return
        if not interaction.guild:
            await interaction.response.send_message("Run this in the server.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True, thinking=False)
        test_ch = await _get_sendable_channel(interaction.guild, self.test_channel_id)
        if test_ch is not None and self.preview_message_ids:
            await _delete_messages_best_effort(test_ch, self.preview_message_ids)
        await interaction.followup.send("Cancelled (preview deleted).", ephemeral=True)


class ComplimentPreviewView(discord.ui.View):
    def __init__(
        self,
        *,
        requester_id: int,
        test_channel_id: int,
        preview_message_ids: list[int],
        reroll_fn,
        publish_fn,
    ):
        super().__init__(timeout=300)
        self.requester_id = requester_id
        self.test_channel_id = int(test_channel_id)
        self.preview_message_ids = list(preview_message_ids)
        self.reroll_fn = reroll_fn
        self.publish_fn = publish_fn

    async def _delete_preview(self, guild: discord.Guild) -> None:
        test_ch = await _get_sendable_channel(guild, self.test_channel_id)
        if test_ch is not None and self.preview_message_ids:
            await _delete_messages_best_effort(test_ch, self.preview_message_ids)

    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.success)
    async def confirm(self, interaction: discord.Interaction, _: discord.ui.Button):
        if interaction.user.id != self.requester_id:
            await interaction.response.send_message("Only the admin who opened this can confirm.", ephemeral=True)
            return
        if not interaction.guild:
            await interaction.response.send_message("Run this in the server.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True, thinking=True)
        await self._delete_preview(interaction.guild)
        result = await self.publish_fn(interaction)
        self.stop()
        await interaction.followup.send(result or "Confirmed and posted.", ephemeral=True)

    @discord.ui.button(label="Pick Another", style=discord.ButtonStyle.primary)
    async def reroll(self, interaction: discord.Interaction, _: discord.ui.Button):
        if interaction.user.id != self.requester_id:
            await interaction.response.send_message("Only the admin who opened this can reroll.", ephemeral=True)
            return
        if not interaction.guild:
            await interaction.response.send_message("Run this in the server.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True, thinking=True)
        await self._delete_preview(interaction.guild)
        self.preview_message_ids, result = await self.reroll_fn(interaction)
        await interaction.followup.send(result, ephemeral=True)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, _: discord.ui.Button):
        if interaction.user.id != self.requester_id:
            await interaction.response.send_message("Only the admin who opened this can cancel.", ephemeral=True)
            return
        if not interaction.guild:
            await interaction.response.send_message("Run this in the server.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True, thinking=False)
        await self._delete_preview(interaction.guild)
        self.stop()
        await interaction.followup.send("Cancelled (preview deleted).", ephemeral=True)


class PredictionAnswerSelect(discord.ui.Select):
    def __init__(
        self,
        *,
        requester_id: int,
        poll_message_id: int,
        poll_channel_id: int,
        answers: list[discord.PollAnswer],
    ):
        options = [
            discord.SelectOption(
                label=_truncate_text(answer.text or f"Option {index}", 100),
                value=str(answer.id),
                description=_truncate_text(f"{answer.vote_count} vote(s)", 100),
            )
            for index, answer in enumerate(answers, start=1)
        ]
        super().__init__(
            placeholder="Choose the correct poll answer…",
            min_values=1,
            max_values=1,
            options=options,
        )
        self.requester_id = int(requester_id)
        self.poll_message_id = int(poll_message_id)
        self.poll_channel_id = int(poll_channel_id)

    async def callback(self, interaction: discord.Interaction) -> None:
        if interaction.user.id != self.requester_id:
            await interaction.response.send_message("Only the admin who opened this can submit the winner.", ephemeral=True)
            return
        if not interaction.guild:
            await interaction.response.send_message("Run this in the server.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        try:
            existing = _get_prediction_row(self.poll_message_id)
        except ValueError as e:
            await interaction.followup.send(str(e), ephemeral=True)
            return

        if existing is not None:
            await interaction.followup.send(
                "That poll is already tracked.\n"
                f"Date: `{existing.get('poll_date', '')}`\n"
                f"Winning answer: {existing.get('winning_answer', '') or '-'}",
                ephemeral=True,
            )
            return

        message = await _find_message_by_id(
            interaction.guild,
            self.poll_message_id,
            channel_id=self.poll_channel_id,
            client=interaction.client,
        )
        if message is None:
            await interaction.followup.send("I couldn't fetch that poll message anymore.", ephemeral=True)
            return

        poll = message.poll
        if poll is None:
            await interaction.followup.send("That message no longer contains a poll.", ephemeral=True)
            return

        answer = poll.get_answer(int(self.values[0]))
        if answer is None:
            await interaction.followup.send("I couldn't find the selected answer on that poll.", ephemeral=True)
            return

        voters: list[discord.abc.User] = []
        try:
            async for voter in answer.voters(limit=None):
                voters.append(voter)
        except discord.DiscordException:
            await interaction.followup.send("I couldn't fetch the poll voters. Check my permissions and try again.", ephemeral=True)
            return

        try:
            all_voters = await _collect_poll_voters(poll)
        except discord.DiscordException:
            await interaction.followup.send("I couldn't fetch the full poll voter list. Check my permissions and try again.", ephemeral=True)
            return

        question = _poll_question_text(poll) or "(Untitled poll)"
        winning_answer = " ".join((answer.text or "").split()) or f"Answer {answer.id}"
        all_people = _format_prediction_people(all_voters)
        right_people = _format_prediction_people(voters)

        try:
            _append_prediction_row(
                {
                    "poll_date": _prediction_date_for_message(message),
                    "message_id": str(message.id),
                    "question": question,
                    "winning_answer": winning_answer,
                    "all_people": all_people,
                    "right_people": right_people,
                }
            )
        except OSError as e:
            await interaction.followup.send(f"Failed to write predictions CSV: {e}", ephemeral=True)
            return

        if self.view is not None:
            self.view.stop()
            try:
                if interaction.message is not None and hasattr(interaction.message, "edit"):
                    await interaction.message.edit(view=None)
            except Exception:
                pass

        people_summary = right_people or "No one voted for the correct answer."
        await interaction.followup.send(
            "Prediction saved.\n"
            f"Question: **{question}**\n"
            f"Winning answer: **{winning_answer}**\n"
            f"Correct voters: {people_summary}\n"
            f"CSV: `{_PREDICTIONS_CSV.relative_to(_REPO_ROOT)}`",
            ephemeral=True,
        )


class PredictionAnswerView(discord.ui.View):
    def __init__(
        self,
        *,
        requester_id: int,
        poll_message_id: int,
        poll_channel_id: int,
        answers: list[discord.PollAnswer],
    ):
        super().__init__(timeout=300)
        self.add_item(
            PredictionAnswerSelect(
                requester_id=requester_id,
                poll_message_id=poll_message_id,
                poll_channel_id=poll_channel_id,
                answers=answers,
            )
        )

    async def on_error(self, interaction: discord.Interaction, error: Exception, item) -> None:
        print("PredictionAnswerView error:", repr(error))
        msg = "Prediction failed while saving. Check terminal logs."
        try:
            if interaction.response.is_done():
                await interaction.followup.send(msg, ephemeral=True)
            else:
                await interaction.response.send_message(msg, ephemeral=True)
        except discord.DiscordException:
            pass


class PredictionPollModal(discord.ui.Modal, title="Prediction"):
    poll_message = discord.ui.TextInput(
        label="Poll message ID or link",
        placeholder="e.g. 1451234567890123456",
        required=True,
        max_length=200,
    )

    async def on_submit(self, interaction: discord.Interaction):
        if not interaction.guild or not interaction.channel:
            await interaction.response.send_message("Run this in the server.", ephemeral=True)
            return

        try:
            await interaction.response.defer(ephemeral=True, thinking=True)
        except discord.NotFound:
            return

        if not config.is_allowed_setup_channel(guild_id=interaction.guild.id, channel_id=interaction.channel.id):
            server = config.server_for_guild_id(interaction.guild.id)
            required = server.setup_channel_id if server else None
            if required is not None:
                await interaction.followup.send(f"Use this in <#{required}>.", ephemeral=True)
                return

        if not interaction.user.guild_permissions.administrator:
            await interaction.followup.send("Admins only.", ephemeral=True)
            return

        try:
            poll_message_id, channel_id = _parse_message_locator(self.poll_message.value)
        except ValueError as e:
            await interaction.followup.send(str(e), ephemeral=True)
            return

        try:
            existing = _get_prediction_row(poll_message_id)
        except ValueError as e:
            await interaction.followup.send(str(e), ephemeral=True)
            return

        if existing is not None:
            await interaction.followup.send(
                "That poll is already tracked.\n"
                f"Date: `{existing.get('poll_date', '')}`\n"
                f"Winning answer: {existing.get('winning_answer', '') or '-'}",
                ephemeral=True,
            )
            return

        message = await _find_message_by_id(
            interaction.guild,
            poll_message_id,
            channel_id=channel_id,
            client=interaction.client,
        )
        if message is None:
            await interaction.followup.send(
                "I couldn't find that poll from the ID alone. If it's in an older or hidden channel, paste the full Discord message link instead.",
                ephemeral=True,
            )
            return

        poll = message.poll
        if poll is None:
            await interaction.followup.send("That message does not contain a Discord poll.", ephemeral=True)
            return

        if not poll.is_finalized():
            await interaction.followup.send(
                "That poll is still open. Close or finalize it first so I can record the final winners.",
                ephemeral=True,
            )
            return

        answers = list(poll.answers)
        if not answers:
            await interaction.followup.send("That poll does not have any answers to choose from.", ephemeral=True)
            return

        question = _poll_question_text(poll) or "(Untitled poll)"
        options_text = "\n".join(
            f"`{index}` {answer.text or f'Answer {answer.id}'}"
            for index, answer in enumerate(answers, start=1)
        )

        embed = discord.Embed(
            title="Prediction",
            description="Choose the correct answer from the dropdown below.",
            color=0xBE629B,
        )
        embed.add_field(name="Question", value=_truncate_text(question, 1024), inline=False)
        embed.add_field(name="Options", value=_truncate_text(options_text, 1024), inline=False)
        embed.add_field(
            name="Poll info",
            value=(
                f"Date: `{_prediction_date_for_message(message)}`\n"
                f"Message ID: `{message.id}`\n"
                f"Channel: {message.channel.mention}"
            ),
            inline=False,
        )

        await interaction.followup.send(
            embed=embed,
            ephemeral=True,
            view=PredictionAnswerView(
                requester_id=interaction.user.id,
                poll_message_id=message.id,
                poll_channel_id=message.channel.id,
                answers=answers,
            ),
        )

    async def on_error(self, interaction: discord.Interaction, error: Exception) -> None:
        print("PredictionPollModal error:", repr(error))
        msg = "Prediction failed while loading the poll. Check terminal logs."
        try:
            if interaction.response.is_done():
                await interaction.followup.send(msg, ephemeral=True)
            else:
                await interaction.response.send_message(msg, ephemeral=True)
        except discord.DiscordException:
            pass


class PredictionResultsModal(discord.ui.Modal, title="Calculate Predictions"):
    month_year = discord.ui.TextInput(
        label="Month / year",
        placeholder="e.g. 03/2026 or 2026-03",
        required=True,
        max_length=20,
    )

    async def on_submit(self, interaction: discord.Interaction):
        if not interaction.guild or not interaction.channel:
            await interaction.response.send_message("Run this in the server.", ephemeral=True)
            return

        try:
            await interaction.response.defer(ephemeral=True, thinking=True)
        except discord.NotFound:
            return

        if not config.is_allowed_setup_channel(guild_id=interaction.guild.id, channel_id=interaction.channel.id):
            server = config.server_for_guild_id(interaction.guild.id)
            required = server.setup_channel_id if server else None
            if required is not None:
                await interaction.followup.send(f"Use this in <#{required}>.", ephemeral=True)
                return

        if not interaction.user.guild_permissions.administrator:
            await interaction.followup.send("Admins only.", ephemeral=True)
            return

        try:
            year, month = _parse_prediction_month(self.month_year.value)
            rows = _load_prediction_rows()
        except ValueError as e:
            await interaction.followup.send(str(e), ephemeral=True)
            return

        correct_counts: Counter[str] = Counter()
        total_counts: Counter[str] = Counter()
        polls_count = 0
        for row in rows:
            poll_date = (row.get("poll_date") or "").strip()
            try:
                parsed_date = datetime.strptime(poll_date, "%Y-%m-%d")
            except ValueError:
                continue

            if parsed_date.year != year or parsed_date.month != month:
                continue

            polls_count += 1
            all_people = set(_parse_prediction_people(row.get("all_people") or ""))
            right_people = set(_parse_prediction_people(row.get("right_people") or ""))

            # Backward compatibility for older rows that were saved before `all_people` existed.
            if not all_people and right_people:
                all_people = set(right_people)

            for user_id in all_people:
                total_counts[user_id] += 1
            for user_id in right_people:
                correct_counts[user_id] += 1

        results = [
            (user_id, correct_counts.get(user_id, 0), total)
            for user_id, total in total_counts.items()
            if total > 0
        ]
        sorted_results = sorted(
            results,
            key=lambda item: (
                -item[1],
                -(item[1] / item[2]),
                -item[2],
                int(item[0]),
            ),
        )
        embed = _build_prediction_results_embed(
            year=year,
            month=month,
            results=sorted_results,
            polls_count=polls_count,
        )

        server = config.server_for_guild_id(interaction.guild.id)
        if server is None:
            await interaction.followup.send(
                "This server is not configured in `config.yaml` (missing matching `SERVER_ID`).",
                ephemeral=True,
            )
            return

        hof_channel_id = _hall_of_fame_channel_id(server)
        if not hof_channel_id:
            await interaction.followup.send(
                "This server is missing `HALL_OF_FAME_CHANNEL_ID` in `config.yaml`.",
                ephemeral=True,
            )
            return

        channel = await _get_sendable_channel(interaction.guild, int(hof_channel_id))
        if channel is None:
            await interaction.followup.send(
                "Couldn't find the Hall of Fame channel. Check `HALL_OF_FAME_CHANNEL_ID` in `config.yaml`.",
                ephemeral=True,
            )
            return

        try:
            await channel.send(
                embed=embed,
                allowed_mentions=discord.AllowedMentions(everyone=False, roles=False, users=True),
            )
        except discord.Forbidden:
            await interaction.followup.send(
                "I don't have permission to post in the Hall of Fame channel.",
                ephemeral=True,
            )
            return

        await interaction.followup.send(f"Posted in <#{int(hof_channel_id)}>.", ephemeral=True)

    async def on_error(self, interaction: discord.Interaction, error: Exception) -> None:
        print("PredictionResultsModal error:", repr(error))
        msg = "Prediction calculation failed. Check terminal logs."
        try:
            if interaction.response.is_done():
                await interaction.followup.send(msg, ephemeral=True)
            else:
                await interaction.response.send_message(msg, ephemeral=True)
        except discord.DiscordException:
            pass

def _find_guild_emoji_by_name(guild: discord.Guild, name: str) -> str:
    want = (name or "").strip().lower()
    if not want:
        return ""
    for e in guild.emojis:
        if (e.name or "").lower() == want:
            return str(e)
    return ""


def _pick_tournament_types(server: config.ServerConfig, *, require_key: str | None = None) -> list[str]:
    """
    Return available tournament type codes (e.g. ["PRT", "ART"]).
    If require_key is set, only return types present in that mapping.
    Falls back to ["PRT", "ART"] if nothing is configured.
    """
    preferred = ["PRT", "ART", "FRT"]
    mapping = None
    if require_key == "tournament_info_channel_id":
        mapping = server.tournament_info_channel_id
    elif require_key == "hall_of_fame_channel_id":
        mapping = server.hall_of_fame_channel_id
    elif require_key == "sponsors_channel_id":
        mapping = server.sponsors_channel_id
    elif require_key == "leaderboard_channel_id":
        mapping = server.leaderboard_channel_id

    if isinstance(mapping, dict) and mapping:
        available = {k.strip().upper() for k in mapping.keys() if str(k).strip()}
        kinds = [k for k in preferred if k in available]
        return kinds or preferred
    return preferred


async def _ensure_team_emoji(guild: discord.Guild, team_name: str) -> str:
    """
    Return the team's custom emoji string if available.
    If missing, best-effort upload it from /icons/teams (requires Manage Emojis permission).
    """
    team = " ".join((team_name or "").strip().split())
    if not team:
        return ""

    existing = emoji_for(team, guild)
    if existing:
        return existing

    icon_path = find_team_icon(team)
    if not icon_path:
        return ""

    emoji_name = emoji_name_for_team(team)[:32]
    if not emoji_name:
        return ""

    try:
        img = icon_path.read_bytes()
        # Discord custom emoji upload limit is small (~256KB). If too large, skip creation.
        if not (0 < len(img) <= 256 * 1024):
            return ""
        created = await guild.create_custom_emoji(
            name=emoji_name,
            image=img,
            reason="Auto-added for sponsor/Hall of Fame post",
        )
        return str(created)
    except (OSError, discord.Forbidden, discord.HTTPException):
        return ""


def _parse_sponsor_line(line: str) -> tuple[str, str, str, str] | tuple[None, str]:
    """
    Parse either:
      1) "Team name | Country | DiscordId"  (default amount 10€)
      2) "Amount | Team name | Country | DiscordId"  (custom amount, e.g. 25€)
      3) (legacy) "<amount> — <team name> <country> <discord id/mention>"

    Returns (team, flag, mention, amount_display) or (None, error).
    """
    s = (line or "").strip()
    if not s:
        return None, "Empty line."

    default_amount = "10€"

    # Preferred format: [Amount |] Team | Country | ID
    if "|" in s:
        parts = [p.strip() for p in s.split("|")]
        if len(parts) == 4:
            amount_display = parts[0] if parts[0] else default_amount
            team, country_raw, uid_raw = parts[1], parts[2], parts[3]
        elif len(parts) == 3:
            amount_display = default_amount
            team, country_raw, uid_raw = parts[0], parts[1], parts[2]
        else:
            return None, f"Expected 3 or 4 parts (e.g. 'Team | Country | ID' or '25€ | Team | Country | ID') in: `{s}`"
        if not team or not country_raw or not uid_raw:
            return None, f"Missing team/country/id in: `{s}`"

        uid = _extract_user_id(uid_raw)
        if not uid:
            return None, f"Couldn't read a Discord user id from: `{uid_raw}`"
        mention = f"<@{uid}>"

        flag = _country_to_flag(country_raw)
        if not flag:
            return None, f"Couldn't read country/flag `{country_raw}` in: `{s}`"

        return team, flag, mention, amount_display

    # Prefer em dash separator.
    if "—" in s:
        amount_raw, rest = (part.strip() for part in s.split("—", 1))
    elif "-" in s:
        amount_raw, rest = (part.strip() for part in s.split("-", 1))
    else:
        return None, f"Missing separator '—' in: `{s}`"

    if not amount_raw or not rest:
        return None, f"Invalid sponsor line: `{s}`"

    uid = _extract_user_id(rest)
    if not uid:
        return None, f"Missing Discord id/mention in: `{s}`"

    mention = f"<@{uid}>"
    amount_display = amount_raw if amount_raw else default_amount

    # Remove uid/mention from rest to parse team + country.
    no_user = _USER_MENTION_RE.sub("", rest).strip()
    no_user = no_user.replace(uid, "", 1).strip()
    if not no_user:
        return None, f"Missing team/country in: `{s}`"

    parts = no_user.split()
    if len(parts) < 2:
        return None, f"Expected: '<team> <country> <id>' in: `{s}`"

    country_raw = parts[-1]
    team = " ".join(parts[:-1]).strip()
    if not team:
        return None, f"Missing team name in: `{s}`"

    flag = _country_to_flag(country_raw)
    if not flag:
        return None, f"Couldn't read country/flag `{country_raw}` in: `{s}`"

    return team, flag, mention, amount_display


def _format_leaderboard_embed(rows: list[dict[str, str]], date_range: str | None = None) -> discord.Embed:
    """
    Build an embed with 3 columns:
      Placement | Team | Points
    Placement supports tie ranges like "1-2" based on equal Points.
    """
    # Keep low so the Team field stays under Discord's 1024-char limit (48 lines).
    max_team = 18

    def to_points_int(s: str) -> int:
        try:
            # Formula points can be decimals; display + tie by rounded integer.
            return int(round(float((s or "").strip() or "0")))
        except ValueError:
            return 0

    # Sort by Points desc, then Team asc for stable display.
    sorted_rows = sorted(
        rows,
        key=lambda r: (-to_points_int(r.get("Points", "")), (r.get("Team") or "").casefold()),
    )

    # Compute placement labels with ties (same Points => same placement range)
    points_list = [to_points_int(r.get("Points", "")) for r in sorted_rows]
    placement_labels = [""] * len(sorted_rows)
    i = 0
    while i < len(sorted_rows):
        j = i
        while j + 1 < len(sorted_rows) and points_list[j + 1] == points_list[i]:
            j += 1
        label = f"{i+1}-{j+1}" if j > i else f"{i+1}"
        for k in range(i, j + 1):
            placement_labels[k] = label
        i = j + 1

    placement_vals: list[str] = []
    team_vals: list[str] = []
    points_vals: list[str] = []

    for idx, r in enumerate(sorted_rows):
        team = " ".join((r.get("Team") or "").split())
        if len(team) > max_team:
            team = team[: max_team - 1] + "…"

        # Replace 1/2/3 with medal emojis in the Placement column (but keep tie ranges like "1-2").
        pl = placement_labels[idx]
        if pl == "1":
            pl = "🥇"
        elif pl == "2":
            pl = "🥈"
        elif pl == "3":
            pl = "🥉"
        placement_vals.append(pl)
        pts_str = str(to_points_int(r.get("Points", "")))

        # Make top 3 teams + points bold (Discord markdown).
        if idx < 3:
            team_vals.append(f"**{team or '-'}**")
            points_vals.append(f"**{pts_str}**")
        else:
            team_vals.append(team or "-")
            points_vals.append(pts_str)

    title_suffix = (date_range or "").strip()
    if title_suffix:
        title = f"Leaderboard ({title_suffix})"
    else:
        title = "Leaderboard"

    e = discord.Embed(title=title, color=0xbe629b)
    e.add_field(name="Placement", value="\n".join(placement_vals) or "-", inline=True)
    e.add_field(name="Team", value="\n".join(team_vals) or "-", inline=True)
    e.add_field(name="Points", value="\n".join(points_vals) or "-", inline=True)
    e.set_footer(text="Best viewed on desktop.")
    return e


def _canonical_team_name(name: str) -> str:
    return " ".join((name or "").strip().split()).casefold()


def _parse_rank_number(value: str) -> int | None:
    raw = (value or "").strip()
    if not raw:
        return None
    if "-" in raw:
        raw = raw.split("-", 1)[0].strip()
    m = _FIRST_INT_RE.search(raw)
    if not m:
        return None
    try:
        return int(m.group(0))
    except ValueError:
        return None


def _points_for_rank(rank: int) -> int:
    for start, end, points in _LEADERBOARD_POINT_RANGES:
        if start <= rank <= end:
            return points
    return 0


def _part_leaderboard_input_dir(tournament_type: str) -> Path:
    ttype = (tournament_type or "").strip().upper()
    if ttype == "PRT":
        return _PART_LEADERBOARD_PRT_DIR
    return _REPO_ROOT / "leaderboard" / f"csv_{ttype.lower()}"


def _load_part_leaderboard_rows(input_dir: Path) -> list[dict[str, str]]:
    if not input_dir.exists() or not input_dir.is_dir():
        raise ValueError(f"Couldn't find `{input_dir.relative_to(_REPO_ROOT)}`.")

    files = sorted(input_dir.glob("*.csv"))
    if not files:
        raise ValueError(f"No CSV files found in `{input_dir.relative_to(_REPO_ROOT)}`.")

    totals: Counter[str] = Counter()
    display_names: dict[str, Counter[str]] = {}

    for file_path in files:
        with file_path.open("r", newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames is None:
                raise ValueError(f"`{file_path.relative_to(_REPO_ROOT)}` has no header row.")

            required_cols = {"Rank", "Team"}
            missing = [c for c in required_cols if c not in set(reader.fieldnames)]
            if missing:
                raise ValueError(
                    f"`{file_path.relative_to(_REPO_ROOT)}` is missing columns: {', '.join(missing)}"
                )

            for row in reader:
                team = " ".join((row.get("Team") or "").split())
                if not team:
                    continue

                rank = _parse_rank_number(row.get("Rank") or "")
                if rank is None:
                    continue

                key = _canonical_team_name(team)
                totals[key] += _points_for_rank(rank)
                display_names.setdefault(key, Counter())[team] += 1

    rows: list[dict[str, str]] = []
    for key, points in totals.items():
        name_counts = display_names.get(key)
        if not name_counts:
            continue
        rows.append({"Team": name_counts.most_common(1)[0][0], "Points": str(int(points))})

    return rows


def _leaderboard_ping(guild: discord.Guild, ping_id: int | None) -> str | None:
    if not ping_id:
        return None
    role = guild.get_role(ping_id)
    return f"<@&{ping_id}>" if role else f"<@{ping_id}>"


async def _send_leaderboard_embed(
    interaction: discord.Interaction,
    *,
    embed: discord.Embed,
    leaderboard_channel_id: int,
    ping_id: int | None,
    preview_label: str,
    test_channel_id: int | None,
    require_preview: bool,
) -> None:
    if not interaction.guild:
        await interaction.followup.send("Run this in the server.", ephemeral=True)
        return

    channel = await _get_sendable_channel(interaction.guild, int(leaderboard_channel_id))
    if channel is None:
        await interaction.followup.send(
            "Couldn't find the leaderboard channel. Check `LEADERBOARD_CHANNEL_ID` in `config.yaml`.",
            ephemeral=True,
        )
        return

    ping = _leaderboard_ping(interaction.guild, ping_id)

    if test_channel_id:
        test_channel = await _get_sendable_channel(interaction.guild, int(test_channel_id))
        if test_channel is None:
            await interaction.followup.send("Couldn't find the test channel.", ephemeral=True)
            return

        try:
            preview_msg = await test_channel.send(
                content=f"[PREVIEW] {preview_label}\n{ping or ''}".strip(),
                embed=embed,
                allowed_mentions=discord.AllowedMentions(everyone=False, roles=False, users=False),
            )
        except discord.Forbidden:
            await interaction.followup.send(
                "I don't have permission to post in the test channel.",
                ephemeral=True,
            )
            return

        async def _publish(confirm_interaction: discord.Interaction) -> str:
            guild = confirm_interaction.guild
            if guild is None:
                return "Run this in the server."

            dest = await _get_sendable_channel(guild, int(leaderboard_channel_id))
            if dest is None:
                return "Couldn't find the leaderboard channel."

            ping2 = _leaderboard_ping(guild, ping_id)
            try:
                await dest.send(
                    content=ping2,
                    embed=embed,
                    allowed_mentions=discord.AllowedMentions(everyone=False, roles=True, users=False),
                )
            except discord.Forbidden:
                return "I don't have permission to post in the leaderboard channel."

            return f"Posted in <#{leaderboard_channel_id}>."

        await interaction.followup.send(
            f"Preview posted in <#{int(test_channel_id)}>. Confirm to post in <#{int(leaderboard_channel_id)}>.",
            ephemeral=True,
            view=ConfirmPostView(
                requester_id=interaction.user.id,
                test_channel_id=int(test_channel_id),
                preview_message_ids=[preview_msg.id],
                publish_fn=_publish,
            ),
        )
        return

    if require_preview:
        await interaction.followup.send(
            "This server is missing `TEST_CHANNEL_ID` in `config.yaml` (needed for previews).",
            ephemeral=True,
        )
        return

    try:
        await channel.send(
            content=ping,
            embed=embed,
            allowed_mentions=discord.AllowedMentions(everyone=False, roles=True, users=False),
        )
    except discord.Forbidden:
        await interaction.followup.send(
            "I don't have permission to post in the leaderboard channel.",
            ephemeral=True,
        )
        return

    await interaction.followup.send(f"Posted in <#{int(leaderboard_channel_id)}>.", ephemeral=True)


def _to_discord_timestamp(raw: str) -> str | None:
    s = raw.strip()
    if not s:
        return None

    m = _TS_RE.fullmatch(s)
    if m:
        return f"<t:{int(m.group(1))}:F>"

    if s.isdigit():
        return f"<t:{int(s)}:F>"

    # Accept "YYYY-MM-DD" or "YYYY-MM-DD HH:MM" (assumed CET/CEST)
    s2 = s.replace("/", "-")
    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M"):
        try:
            dt = datetime.strptime(s2, fmt).replace(tzinfo=_CET)
            return f"<t:{int(dt.astimezone(timezone.utc).timestamp())}:F>"
        except ValueError:
            pass

    return s


def _split_entry_prize(raw: str) -> tuple[str, str] | None:
    # Expect: "Entry / Prize" (also accepts "|" as separator)
    s = raw.strip()
    if not s:
        return None

    if "|" in s:
        a, b = (part.strip() for part in s.split("|", 1))
    elif "/" in s:
        a, b = (part.strip() for part in s.split("/", 1))
    else:
        return None

    if not a or not b:
        return None
    return a, b


def _split_entry_prize_and_time(raw: str) -> tuple[str, str, str] | None:
    """
    Discord Modals support max 5 TextInputs, so we combine:
      Entry fee | Prize pool | Date & time

    Accepted separators: "|" or "/"
    Examples:
      "€10 | €200 | 2026-02-11 19:00"
      "€10 / €200 / <t:1739300400>"
    """
    s = raw.strip()
    if not s:
        return None

    sep = "|" if "|" in s else ("/" if "/" in s else None)
    if not sep:
        return None

    parts = [p.strip() for p in s.split(sep)]
    if len(parts) < 3:
        return None

    entry = parts[0]
    prize = parts[1]
    when_raw = sep.join(parts[2:]).strip()  # allow separators in the timestamp string
    if not entry or not prize or not when_raw:
        return None

    return entry, prize, when_raw


def _split_org_and_name(raw: str) -> tuple[str, str] | None:
    # Expect: "ORG | Tournament name"
    s = raw.strip()
    if "|" not in s:
        return None
    org, name = (part.strip() for part in s.split("|", 1))
    if not org or not name:
        return None
    return org, name


def _flag_from_iso2(code: str) -> str | None:
    c = (code or "").strip().upper()
    if len(c) != 2 or not c.isalpha():
        return None
    a = ord(c[0]) - ord("A")
    b = ord(c[1]) - ord("A")
    if not (0 <= a <= 25 and 0 <= b <= 25):
        return None
    return chr(0x1F1E6 + a) + chr(0x1F1E6 + b)


def _extract_user_id(raw: str) -> str | None:
    s = (raw or "").strip()
    if not s:
        return None

    m = _USER_MENTION_RE.search(s)
    if m:
        return m.group(1)

    # Accept raw numeric IDs too.
    if s.isdigit():
        return s

    m2 = re.search(r"(\d{15,20})", s)
    if m2:
        return m2.group(1)

    return None


def _country_to_flag(raw: str) -> str | None:
    s = (raw or "").strip()
    if not s:
        return None

    # If they paste the actual flag emoji, keep it.
    # (Flags are two "regional indicator" codepoints; we just accept common 2-char sequences.)
    if len(s) <= 4 and any("\U0001F1E6" <= ch <= "\U0001F1FF" for ch in s):
        return s

    # Support :flag_fr: style.
    m = _FLAG_ALIAS_RE.match(s)
    if m:
        return _flag_from_iso2(m.group(1))

    # Support ISO-2 codes like FR, GB, US, etc.
    if len(s) == 2 and s.isalpha():
        return _flag_from_iso2(s)

    # Support a few common country names.
    name = " ".join(s.casefold().split())
    common = {
        "france": "FR",
        "french": "FR",
        "germany": "DE",
        "deutschland": "DE",
        "serbia": "RS",
        "spain": "ES",
        "hungary": "HU",
        "italy": "IT",
        "portugal": "PT",
        "netherlands": "NL",
        "holland": "NL",
        "belgium": "BE",
        "switzerland": "CH",
        "austria": "AT",
        "sweden": "SE",
        "norway": "NO",
        "denmark": "DK",
        "finland": "FI",
        "poland": "PL",
        "czech republic": "CZ",
        "czechia": "CZ",
        "romania": "RO",
        "bulgaria": "BG",
        "greece": "GR",
        "turkey": "TR",
        "ukraine": "UA",
        "russia": "RU",
        "united kingdom": "GB",
        "uk": "GB",
        "england": "GB",
        "scotland": "GB",
        "wales": "GB",
        "ireland": "IE",
        "united states": "US",
        "usa": "US",
        "canada": "CA",
        "mexico": "MX",
        "brazil": "BR",
        "argentina": "AR",
        "chile": "CL",
        "colombia": "CO",
        "peru": "PE",
        "japan": "JP",
        "china": "CN",
        "south korea": "KR",
        "korea": "KR",
        "india": "IN",
        "australia": "AU",
        "new zealand": "NZ",
        "saudi arabia": "SA",
        "morocco": "MA",
        "tunisia": "TN",
        "algeria": "DZ",
        "egypt": "EG",
        "south africa": "ZA",
    }
    if name in common:
        return _flag_from_iso2(common[name])

    return None


async def _ensure_team_role(
    guild: discord.Guild,
    *,
    role_name: str,
    team_name: str,
    role_colors: list[int] | None = None,
    position_offset: int | None = None,
) -> discord.Role | None:
    """
    Ensure a hoisted + mentionable role exists for the team.
    Colors the role using gradient colors from `colors` list in `rosters.yaml` (best-effort).
    If two colors are provided, creates a gradient role (first color on left, second on right).
    Attaches a role icon from the team's custom emoji if available.
    If position_offset is set (e.g. 1, 2, 3), the role is placed above MINIMUM_ROLE_ID in that slot.
    """
    desired = " ".join((role_name or "").strip().split())
    legacy = " ".join((team_name or "").strip().split())
    if not desired or not legacy:
        return None

    # Prefer the rank-prefixed role name.
    role = discord.utils.get(guild.roles, name=desired)
    if role is not None:
        # Ensure existing role is above MINIMUM_ROLE_ID if we have an offset
        if position_offset is not None:
            server_cfg = config.server_for_guild_id(guild.id)
            if server_cfg and server_cfg.minimum_role_id:
                minimum_role = guild.get_role(server_cfg.minimum_role_id)
                if minimum_role and role.position <= minimum_role.position:
                    try:
                        await role.edit(
                            position=minimum_role.position + position_offset,
                            reason="Positioned above MINIMUM_ROLE_ID",
                        )
                    except (discord.Forbidden, discord.HTTPException):
                        pass
        return role

    # Back-compat: if an old role exists with just the team name, reuse it and rename (best-effort).
    role = discord.utils.get(guild.roles, name=legacy)
    if role is not None and role.name != desired:
        try:
            # Try to update icon when renaming
            emoji_name = emoji_name_for_team(team_name)
            role_icon = None
            if emoji_name:
                emoji_obj = _find_custom_emoji(guild, emoji_name)
                if emoji_obj:
                    # Fetch emoji image bytes for role icon
                    try:
                        async with httpx.AsyncClient() as client:
                            response = await client.get(str(emoji_obj.url))
                            if response.status_code == 200:
                                role_icon = response.content
                    except Exception:
                        # If fetching fails, continue without icon
                        pass
            
            # Parse gradient colors if provided
            primary_color = None
            secondary_color = None
            if role_colors and len(role_colors) >= 1:
                try:
                    primary_color = int(str(role_colors[0]).strip(), 0)
                    if len(role_colors) >= 2:
                        secondary_color = int(str(role_colors[1]).strip(), 0)
                except (ValueError, TypeError):
                    pass
            
            # Check if we need to position the role above MINIMUM_ROLE_ID
            server_cfg = config.server_for_guild_id(guild.id)
            position = None
            if position_offset is not None and server_cfg and server_cfg.minimum_role_id:
                minimum_role = guild.get_role(server_cfg.minimum_role_id)
                if minimum_role and role.position <= minimum_role.position:
                    position = minimum_role.position + position_offset
            
            edit_kwargs = {
                "name": desired,
                "display_icon": role_icon,
                "reason": "Auto-renamed team role for rosters",
            }
            if position is not None:
                edit_kwargs["position"] = position
            
            # Update colors: gradient if both colors provided, solid if one, or keep existing if none
            if primary_color is not None and secondary_color is not None:
                # Set gradient colors via HTTP API
                try:
                    await guild._state.http.edit_role(
                        guild.id,
                        role.id,
                        colors={
                            "primary_color": primary_color,
                            "secondary_color": secondary_color,
                            "tertiary_color": None,
                        },
                        reason="Set gradient colors for team role",
                    )
                except Exception:
                    # If gradient fails, try solid color
                    if primary_color is not None:
                        edit_kwargs["colour"] = discord.Colour(primary_color)
            elif primary_color is not None:
                edit_kwargs["colour"] = discord.Colour(primary_color)
            
            await role.edit(**edit_kwargs)
        except (discord.Forbidden, discord.HTTPException):
            # If we can't rename, we'll just use the legacy role.
            pass
        return role

    # Parse gradient colors: first color on left, second on right
    primary_color = None
    secondary_color = None
    if role_colors and len(role_colors) >= 1:
        try:
            primary_color = int(str(role_colors[0]).strip(), 0)
            if len(role_colors) >= 2:
                secondary_color = int(str(role_colors[1]).strip(), 0)
        except (ValueError, TypeError):
            primary_color = None
            secondary_color = None
    
    # Fallback to solid color if only one color provided
    colour = discord.Colour(primary_color) if primary_color is not None else discord.Colour.default()

    # Find the team's custom emoji for the role icon
    emoji_name = emoji_name_for_team(team_name)
    role_icon = None
    if emoji_name:
        emoji_obj = _find_custom_emoji(guild, emoji_name)
        if emoji_obj:
            # Fetch emoji image bytes for role icon
            try:
                async with httpx.AsyncClient() as client:
                    response = await client.get(str(emoji_obj.url))
                    if response.status_code == 200:
                        role_icon = response.content
            except Exception:
                # If fetching fails, continue without icon
                pass

    try:
        # Build create_role kwargs
        create_kwargs = {
            "name": desired,
            "hoist": True,  # displayed separately on the right
            "mentionable": True,
            "display_icon": role_icon,
            "reason": "Auto-created team role for rosters",
        }
        
        # Use gradient colors if both are provided, otherwise use solid color
        if primary_color is not None and secondary_color is not None:
            # Create role with primary color first
            role = await guild.create_role(
                **create_kwargs,
                colour=colour,  # Set primary color as fallback
            )
            # Set gradient colors via HTTP API
            if role:
                try:
                    await guild._state.http.edit_role(
                        guild.id,
                        role.id,
                        colors={
                            "primary_color": primary_color,
                            "secondary_color": secondary_color,
                            "tertiary_color": None,
                        },
                        reason="Set gradient colors for team role",
                    )
                except Exception:
                    # If gradient fails, role will have solid color
                    pass
        else:
            # Single color - use solid color
            role = await guild.create_role(**create_kwargs, colour=colour)
        
        # Position the role above MINIMUM_ROLE_ID if configured (each team gets its own slot)
        if role and position_offset is not None:
            server_cfg = config.server_for_guild_id(guild.id)
            if server_cfg and server_cfg.minimum_role_id:
                minimum_role = guild.get_role(server_cfg.minimum_role_id)
                if minimum_role and role.position <= minimum_role.position:
                    try:
                        await role.edit(
                            position=minimum_role.position + position_offset,
                            reason="Positioned above MINIMUM_ROLE_ID",
                        )
                    except (discord.Forbidden, discord.HTTPException):
                        pass
    except discord.Forbidden:
        return None
    except discord.HTTPException:
        return None

    return role


def _parse_winning_roster(raw: str) -> tuple[list[str], str | None]:
    """
    Input: one player per line:
      <discord id or mention> <country>

    Country accepted as:
      - 🇫🇷 (flag emoji)
      - :flag_fr:
      - FR (ISO-2)
      - France (common names only)

    Output lines: "🇫🇷 <@123...>"
    Returns (lines, error_message)
    """
    lines_in = (raw or "").splitlines()
    out: list[str] = []
    for ln in lines_in:
        line = ln.strip()
        if not line:
            continue

        # Grab user id first, then treat the rest as country text.
        uid = _extract_user_id(line)
        if not uid:
            return [], f"Couldn't read a Discord user id from: `{line}`"

        # Remove the mention/id chunk to get country.
        rest = _USER_MENTION_RE.sub("", line).strip()
        rest = rest.replace(uid, "", 1).strip()
        if not rest:
            return [], f"Missing country for: `<@{uid}>` (line: `{line}`)"

        flag = _country_to_flag(rest)
        if not flag:
            return [], (
                f"Couldn't read a country/flag from: `{rest}`.\n"
                "Use `FR`, `:flag_fr:`, or `🇫🇷` (or a common country name like `France`)."
            )

        out.append(f"{flag} <@{uid}>")

    if not out:
        return [], "Winning roster is required (at least 1 player line)."

    return out, None


def _parse_roster(raw: str) -> tuple[list[str], str | None]:
    """
    Input: one player per line, in either order:
      <country> <discord id or mention>
      <discord id or mention> <country>

    Country accepted as:
      - 🇫🇷 (flag emoji)
      - :flag_fr:
      - FR (ISO-2)
      - common country names (limited list)

    Output lines: "🇫🇷 <@123...>"
    Returns (lines, error_message)
    """
    lines_in = (raw or "").splitlines()
    out: list[str] = []
    for ln in lines_in:
        line = ln.strip()
        if not line:
            continue

        uid = _extract_user_id(line)
        if not uid:
            return [], f"Couldn't read a Discord user id from: `{line}`"

        # Remove the mention/id chunk to get country.
        rest = _USER_MENTION_RE.sub("", line).strip()
        rest = rest.replace(uid, "", 1).strip()
        if not rest:
            return [], f"Missing country for: `<@{uid}>` (line: `{line}`)"

        flag = _country_to_flag(rest)
        if not flag:
            return [], (
                f"Couldn't read a country/flag from: `{rest}`.\n"
                "Use `FR`, `:flag_fr:`, or `🇫🇷` (or a common country name like `France`)."
            )

        out.append(f"{flag} <@{uid}>")

    if not out:
        return [], "Roster is required (at least 1 player line)."

    return out, None


class TournamentResultsModal(discord.ui.Modal, title="Tournament Results"):
    tournament_name = discord.ui.TextInput(
        label="Tournament (ORG | Name)",
        placeholder="e.g. MRC | Rematch Weekly #12",
        required=True,
        max_length=80,
    )
    tournament_url = discord.ui.TextInput(
        label="Tournament URL",
        placeholder="https://...",
        required=True,
        max_length=200,
    )
    entry_and_prize = discord.ui.TextInput(
        label="Entry | Prize | Date & time",
        placeholder="e.g. €10 | €200 | 2026-02-11 19:00",
        required=True,
        max_length=120,
    )
    standings = discord.ui.TextInput(
        label="Standings (top 4)",
        placeholder="Team A\nTeam B\nTeam C\nTeam D",
        style=discord.TextStyle.paragraph,
        required=True,
        max_length=400,
    )
    winning_roster = discord.ui.TextInput(
        label="Winning roster",
        placeholder="One per line: <@id> FR  (or :flag_fr: / 🇫🇷)",
        style=discord.TextStyle.paragraph,
        required=True,
        max_length=400,
    )

    async def on_submit(self, interaction: discord.Interaction):
        org_and_name = _split_org_and_name(self.tournament_name.value or "")
        if not org_and_name:
            await interaction.response.send_message(
                "Tournament format: `MRC | Rematch Weekly #12`",
                ephemeral=True,
            )
            return
        t_org, t_name = org_and_name
        t_url = (self.tournament_url.value or "").strip()
        entry_prize_time = _split_entry_prize_and_time(self.entry_and_prize.value or "")
        if not entry_prize_time:
            await interaction.response.send_message(
                "Entry/Prize/Date format: `€10 | €200 | 2026-02-11 19:00`",
                ephemeral=True,
            )
            return
        t_entry, t_prize, when_raw = entry_prize_time
        t_when = _to_discord_timestamp(when_raw)

        raw_lines = (self.standings.value or "").splitlines()
        teams = [line.strip() for line in raw_lines if line.strip()][:4]
        medals = ["1.", "2.", "3.", "4."]

        roster_lines, roster_err = _parse_winning_roster(self.winning_roster.value or "")
        if roster_err:
            await interaction.response.send_message(
                "Winning roster format (one per line):\n"
                "`<@123456789012345678> FR`\n"
                "`123456789012345678 :flag_fr:`\n"
                "`<@123456789012345678> 🇫🇷`\n\n"
                f"{roster_err}",
                ephemeral=True,
            )
            return

        lines: list[str] = []
        for i, name in enumerate(teams):
            medal = medals[i]
            e = emoji_for(name, interaction.guild)
            lines.append(f"{medal} {e + ' ' if e else ''}{name}")

        embed = discord.Embed(title=t_name, color=0xbe629b)
        embed.add_field(name="Tournament", value=f"[URL]({t_url})", inline=True)
        embed.add_field(name="Entry fee", value=t_entry, inline=True)
        embed.add_field(name="Prize pool", value=t_prize, inline=True)
        embed.add_field(name="Date & time", value=t_when, inline=False)
        embed.add_field(name="Standings", value="\n".join(lines) or "-", inline=False)
        embed.add_field(name="Winning roster", value="\n".join(roster_lines), inline=False)

        icon_path = find_icon(t_org)
        icon_file = None
        if icon_path:
            filename = icon_path.name
            icon_file = discord.File(icon_path, filename=filename)
            embed.set_thumbnail(url=f"attachment://{filename}")

        if not interaction.guild:
            await interaction.response.send_message("Run this in the server.", ephemeral=True)
            return

        server = config.server_for_guild_id(interaction.guild.id)
        results_channel_id = server.results_tournaments_channel_id if server else None
        if not results_channel_id:
            await interaction.response.send_message(
                "This server is missing `RESULTS_TOURNAMENTS_CHANNEL_ID` in `config.yaml`.",
                ephemeral=True,
            )
            return

        test_channel_id = server.test_channel_id if server else None
        if not test_channel_id:
            await interaction.response.send_message(
                "This server is missing `TEST_CHANNEL_ID` in `config.yaml` (needed for previews).",
                ephemeral=True,
            )
            return

        test_channel = await _get_sendable_channel(interaction.guild, int(test_channel_id))
        if test_channel is None:
            await interaction.response.send_message("Couldn't find the test channel.", ephemeral=True)
            return

        # Post preview first (no pings).
        await interaction.response.defer(ephemeral=True, thinking=True)
        preview_kwargs = dict(
            content="[PREVIEW] Tournament Results",
            embed=embed,
            allowed_mentions=discord.AllowedMentions(everyone=False, roles=False, users=False),
        )
        if icon_path:
            preview_kwargs["file"] = discord.File(icon_path, filename=icon_path.name)
        preview_msg = await test_channel.send(**preview_kwargs)

        async def _publish(confirm_interaction: discord.Interaction) -> str:
            guild = confirm_interaction.guild
            if guild is None:
                return "Run this in the server."
            dest = await _get_sendable_channel(guild, int(results_channel_id))
            if dest is None:
                return "Couldn't find the results channel."

            ping_id = server.tournaments_ping_id if server else None
            content = None
            if ping_id:
                role = guild.get_role(ping_id)
                content = f"<@&{ping_id}>" if role else f"<@{ping_id}>"

            kwargs = dict(
                content=content,
                embed=embed,
                allowed_mentions=discord.AllowedMentions(everyone=False, roles=True, users=True),
            )
            if icon_path:
                kwargs["file"] = discord.File(icon_path, filename=icon_path.name)
            msg = await dest.send(**kwargs)

            # React with winner + organizer emojis (best-effort).
            winner_team = teams[0] if teams else ""
            winner_emoji = emoji_for(winner_team, guild)
            org_emoji = emoji_for_org(t_org, guild)
            for r in (winner_emoji, org_emoji):
                if not r:
                    continue
                try:
                    await msg.add_reaction(r)
                except discord.DiscordException:
                    pass

            return f"Posted in <#{results_channel_id}>."

        await interaction.followup.send(
            f"Preview posted in <#{test_channel_id}>. Confirm to post in <#{results_channel_id}>.",
            ephemeral=True,
            view=ConfirmPostView(
                requester_id=interaction.user.id,
                test_channel_id=int(test_channel_id),
                preview_message_ids=[preview_msg.id],
                publish_fn=_publish,
            ),
        )
        return

        # (Posting now happens only after confirmation.)

    async def on_error(self, interaction: discord.Interaction, error: Exception) -> None:
        print("TournamentResultsModal error:", repr(error))
        msg = "Something went wrong while creating the results embed."
        if interaction.response.is_done():
            await interaction.followup.send(msg, ephemeral=True)
        else:
            await interaction.response.send_message(msg, ephemeral=True)


class TournamentInfoModal(discord.ui.Modal):
    tournament_name = discord.ui.TextInput(
        label="Tournament name",
        placeholder="e.g. PRT #9 — Rematch Weekly Cup",
        required=True,
        max_length=100,
    )
    battlefy_url = discord.ui.TextInput(
        label="Battlefy URL",
        placeholder="https://battlefy.com/...",
        required=True,
        max_length=200,
    )
    date_time = discord.ui.TextInput(
        label="Date & time",
        placeholder="e.g. 2026-02-11 19:00  (CET)  or  <t:1739300400>",
        required=True,
        max_length=80,
    )
    prize_pool_input = discord.ui.TextInput(
        label="Prize pool",
        placeholder="e.g. 50€ (leave blank to use default)",
        required=False,
        max_length=40,
    )

    def __init__(self, *, tournament_type: str):
        self.tournament_type = (tournament_type or "").strip().upper()
        super().__init__(title=f"{self.tournament_type} Tournament Info")

    async def on_submit(self, interaction: discord.Interaction):
        t_name = " ".join((self.tournament_name.value or "").strip().split())
        t_url = (self.battlefy_url.value or "").strip()
        when = _to_discord_timestamp(self.date_time.value or "")
        prize_pool_raw = " ".join((self.prize_pool_input.value or "").strip().split())

        if not interaction.guild:
            await interaction.response.send_message("Run this in the server.", ephemeral=True)
            return

        server = config.server_for_guild_id(interaction.guild.id)
        if server is None:
            await interaction.response.send_message(
                "This server is not configured in `config.yaml` (missing matching `SERVER_ID`).",
                ephemeral=True,
            )
            return

        ttype = self.tournament_type or "PRT"
        # Destination channel
        info_channel_id = (server.tournament_info_channel_id or {}).get(ttype)
        if not info_channel_id:
            await interaction.response.send_message(
                f"This server is missing `TOURNAMENT_INFO_CHANNEL_ID.{ttype}` in `config.yaml`.",
                ephemeral=True,
            )
            return

        await interaction.response.defer(ephemeral=True)

        # Embed color
        color = (server.embed_color or {}).get(ttype, 0xbe629b)
        # Prize pool
        default_prize_pool = (server.prize_pool or {}).get(ttype, 50.0)
        if prize_pool_raw:
            prize_pool_display = prize_pool_raw
        else:
            prize_pool_display = f"{default_prize_pool:g}€"

        embed = discord.Embed(title=t_name or "Tournament", color=int(color))
        # Row 1 (inline): Battlefy | Rules | Fees & Rewards
        embed.add_field(name="Battlefy", value=f"[URL]({t_url})" if t_url else "-", inline=True)
        embed.add_field(name="Rules", value=f"[URL]({_RULEBOOK_URL})", inline=True)
        embed.add_field(
            name="Fees & Rewards",
            value=f"__Entry Fee__: 0€\n__Prize Pool__: {prize_pool_display}",
            inline=True,
        )
        # Rows below (stacked)
        embed.add_field(
            name="Date & time",
            value=(f"{when}\nRegistration closes 10 minutes before tournament start").strip(),
            inline=False,
        )
        embed.add_field(
            name="Format",
            value=(
                "Double Elimination Bracket\n"
                "__Winners Bracket__: BO3\n"
                "__Losers Bracket__: BO1\n"
                "__Grand Final__: BO5 with a 1-game advantage for the Winners Bracket team"
            ),
            inline=False,
        )
        embed.add_field(
            name="Match Settings",
            value=(
                "```"
                "Match duration: 6 min (WB) | 8 min (LB)\n"
                "Overtime max duration: Infinite\n"
                "Score to reach: 0\n"
                "Mercy rule goal difference: 4\n"
                "Enable goal sweeper: No"
                "```"
            ),
            inline=False,
        )

        icon_path = find_icon(ttype)
        icon_file = None
        if icon_path:
            filename = icon_path.name
            icon_file = discord.File(icon_path, filename=filename)
            embed.set_thumbnail(url=f"attachment://{filename}")

        channel = interaction.guild.get_channel(info_channel_id)
        if channel is None:
            try:
                channel = await interaction.guild.fetch_channel(info_channel_id)
            except discord.DiscordException:
                channel = None

        if channel is None or not hasattr(channel, "send"):
            await interaction.followup.send(
                f"Couldn't find the tournament-info channel. Check `TOURNAMENT_INFO_CHANNEL_ID.{ttype}` in `config.yaml`.",
                ephemeral=True,
            )
            return

        try:
            kwargs = dict(
                embed=embed,
                allowed_mentions=discord.AllowedMentions(everyone=False, roles=False, users=False),
            )
            if icon_file:
                kwargs["file"] = icon_file
            await channel.send(**kwargs)
        except discord.Forbidden:
            await interaction.followup.send(
                "I don't have permission to post in the tournaments channel.",
                ephemeral=True,
            )
            return

        await interaction.followup.send(
            f"Posted in <#{channel.id}>.",
            ephemeral=True,
        )

    async def on_error(self, interaction: discord.Interaction, error: Exception) -> None:
        print("TournamentInfoModal error:", repr(error))
        msg = "Something went wrong while creating the tournament info embed."
        try:
            if interaction.response.is_done():
                await interaction.followup.send(msg, ephemeral=True)
            else:
                await interaction.response.send_message(msg, ephemeral=True)
        except discord.DiscordException:
            pass


class FRTTournamentInfoModal(discord.ui.Modal):
    """Tournament info modal for FRT: edition, Battlefy URL, Date & Time, Mode, Format."""

    edition_number = discord.ui.TextInput(
        label="Edition number",
        placeholder="e.g. 1",
        required=True,
        max_length=10,
    )
    battlefy_url = discord.ui.TextInput(
        label="Battlefy URL",
        placeholder="https://battlefy.com/...",
        required=True,
        max_length=200,
    )
    date_time = discord.ui.TextInput(
        label="Date & time",
        placeholder="e.g. 2026-02-11 19:00 (CET) or <t:1739300400>",
        required=True,
        max_length=80,
    )
    mode = discord.ui.TextInput(
        label="Mode",
        placeholder="e.g. Rondo (4v4)",
        required=True,
        max_length=80,
    )
    format_input = discord.ui.TextInput(
        label="Format",
        placeholder="e.g. Single Elimination BO3",
        required=True,
        max_length=300,
        style=discord.TextStyle.paragraph,
    )

    def __init__(self) -> None:
        super().__init__(title="FRT Tournament Info")

    async def on_submit(self, interaction: discord.Interaction):
        ed_raw = (self.edition_number.value or "").strip()
        m = re.search(r"\d+", ed_raw)
        if not m:
            await interaction.response.send_message(
                "Edition number must contain at least one number.",
                ephemeral=True,
            )
            return
        edition = int(m.group(0))
        t_url = (self.battlefy_url.value or "").strip()
        when = _to_discord_timestamp(self.date_time.value or "")
        if when is None:
            when = (self.date_time.value or "").strip() or "-"
        mode_val = (self.mode.value or "").strip() or "-"
        format_val = (self.format_input.value or "").strip() or "-"

        if not interaction.guild:
            await interaction.response.send_message("Run this in the server.", ephemeral=True)
            return

        server = config.server_for_guild_id(interaction.guild.id)
        if server is None:
            await interaction.response.send_message(
                "This server is not configured in `config.yaml` (missing matching `SERVER_ID`).",
                ephemeral=True,
            )
            return

        ttype = "FRT"
        info_channel_id = (server.tournament_info_channel_id or {}).get(ttype)
        if not info_channel_id:
            await interaction.response.send_message(
                f"This server is missing `TOURNAMENT_INFO_CHANNEL_ID.{ttype}` in `config.yaml`.",
                ephemeral=True,
            )
            return

        await interaction.response.defer(ephemeral=True)

        color = (server.embed_color or {}).get(ttype, 0xbe629b)

        embed = discord.Embed(title=f"FRT #{edition}", color=int(color))
        embed.add_field(name="Battlefy", value=f"[URL]({t_url})" if t_url else "-", inline=True)
        embed.add_field(name="Entry Fee", value="0€", inline=True)
        embed.add_field(name="Prize Pool", value="0€", inline=True)
        embed.add_field(
            name="Date & time",
            value=f"{when}\nRegistration closes 1 minute before tournament start.",
            inline=False,
        )
        embed.add_field(
            name="Mode",
            value=f"{mode_val} — [Rules]({_FRT_RULES_URL})",
            inline=False,
        )
        embed.add_field(name="Format", value=format_val, inline=False)

        icon_path = find_icon(ttype)
        icon_file = None
        if icon_path:
            filename = icon_path.name
            icon_file = discord.File(icon_path, filename=filename)
            embed.set_thumbnail(url=f"attachment://{filename}")

        channel = interaction.guild.get_channel(info_channel_id)
        if channel is None:
            try:
                channel = await interaction.guild.fetch_channel(info_channel_id)
            except discord.DiscordException:
                channel = None

        if channel is None or not hasattr(channel, "send"):
            await interaction.followup.send(
                f"Couldn't find the tournament-info channel. Check `TOURNAMENT_INFO_CHANNEL_ID.{ttype}` in `config.yaml`.",
                ephemeral=True,
            )
            return

        try:
            kwargs = dict(
                embed=embed,
                allowed_mentions=discord.AllowedMentions(everyone=False, roles=False, users=False),
            )
            if icon_file:
                kwargs["file"] = icon_file
            await channel.send(**kwargs)
        except discord.Forbidden:
            await interaction.followup.send(
                "I don't have permission to post in the tournaments channel.",
                ephemeral=True,
            )
            return

        await interaction.followup.send(
            f"Posted in <#{channel.id}>.",
            ephemeral=True,
        )

    async def on_error(self, interaction: discord.Interaction, error: Exception) -> None:
        print("FRTTournamentInfoModal error:", repr(error))
        msg = "Something went wrong while creating the tournament info embed."
        try:
            if interaction.response.is_done():
                await interaction.followup.send(msg, ephemeral=True)
            else:
                await interaction.response.send_message(msg, ephemeral=True)
        except discord.DiscordException:
            pass


class TournamentInfoTypeSelect(discord.ui.Select):
    def __init__(self, *, options: list[discord.SelectOption]):
        super().__init__(
            placeholder="Select tournament type…",
            min_values=1,
            max_values=1,
            options=options,
        )

    async def callback(self, interaction: discord.Interaction):
        ttype = (self.values[0] if self.values else "").strip().upper()
        if ttype == "FRT":
            await interaction.response.send_modal(FRTTournamentInfoModal())
        else:
            await interaction.response.send_modal(TournamentInfoModal(tournament_type=ttype))


class TournamentInfoTypeView(discord.ui.View):
    def __init__(self, *, options: list[discord.SelectOption]):
        super().__init__(timeout=120)
        self.add_item(TournamentInfoTypeSelect(options=options))


class HallOfFameModal(discord.ui.Modal):
    edition_number = discord.ui.TextInput(
        label="Edition number",
        placeholder="e.g. 9",
        required=True,
        max_length=10,
    )
    team_name = discord.ui.TextInput(
        label="Team name",
        placeholder="e.g. OVERDOZEE",
        required=True,
        max_length=60,
    )
    bracket_url = discord.ui.TextInput(
        label="Bracket URL",
        placeholder="https://battlefy.com/...",
        required=True,
        max_length=200,
    )
    roster = discord.ui.TextInput(
        label="Roster (one per line: country + discord id)",
        placeholder="FR 123456789012345678\nMA <@123456789012345678>\n:flag_es: 123456789012345678",
        style=discord.TextStyle.paragraph,
        required=True,
        max_length=600,
    )

    def __init__(self, *, tournament_type: str):
        self.tournament_type = (tournament_type or "").strip().upper()
        super().__init__(title=f"{self.tournament_type} Hall of Fame")

    async def on_submit(self, interaction: discord.Interaction):
        if not interaction.guild:
            await interaction.response.send_message("Run this in the server.", ephemeral=True)
            return

        server = config.server_for_guild_id(interaction.guild.id)
        if server is None:
            await interaction.response.send_message(
                "This server is not configured in `config.yaml` (missing matching `SERVER_ID`).",
                ephemeral=True,
            )
            return

        ttype = self.tournament_type or "PRT"
        hof_channel_id = _hall_of_fame_channel_id(server, tournament_type=ttype)
        if not hof_channel_id:
            await interaction.response.send_message(
                f"This server is missing `HALL_OF_FAME_CHANNEL_ID.{ttype}` in `config.yaml`.",
                ephemeral=True,
            )
            return

        # Validate edition number
        ed_raw = (self.edition_number.value or "").strip()
        m = re.search(r"\d+", ed_raw)
        if not m:
            await interaction.response.send_message("Edition number must contain a number (e.g. `9`).", ephemeral=True)
            return
        edition = int(m.group(0))

        team = " ".join((self.team_name.value or "").strip().split())
        url = (self.bracket_url.value or "").strip()

        roster_lines, roster_err = _parse_roster(self.roster.value or "")
        if roster_err:
            await interaction.response.send_message(
                "Roster format (one per line):\n"
                "`FR 123456789012345678`\n"
                "`:flag_fr: <@123456789012345678>`\n"
                "`🇫🇷 123456789012345678`\n\n"
                f"{roster_err}",
                ephemeral=True,
            )
            return

        color = (server.embed_color or {}).get(ttype, 0xbe629b)

        # Attach TEAM icon as the main embed image (not thumbnail).
        team_icon_path = find_team_icon(team)
        team_icon_file = None

        # Try to use (or best-effort create) the custom emoji.
        team_emoji = await _ensure_team_emoji(interaction.guild, team)

        title = f"{ttype} #{edition} Champions — {team}{(' ' + team_emoji) if team_emoji else ''}"
        embed = discord.Embed(title=title, color=int(color))
        embed.add_field(name="Bracket", value=f"[Battlefy]({url})" if url else "-", inline=False)
        embed.add_field(name="Roster", value="\n".join(roster_lines) or "-", inline=False)

        if team_icon_path:
            filename = team_icon_path.name
            team_icon_file = discord.File(team_icon_path, filename=filename)
            embed.set_image(url=f"attachment://{filename}")

        channel = interaction.guild.get_channel(hof_channel_id)
        if channel is None:
            try:
                channel = await interaction.guild.fetch_channel(hof_channel_id)
            except discord.DiscordException:
                channel = None

        if channel is None or not hasattr(channel, "send"):
            await interaction.response.send_message(
                f"Couldn't find the Hall of Fame channel. Check `HALL_OF_FAME_CHANNEL_ID.{ttype}` in `config.yaml`.",
                ephemeral=True,
            )
            return

        try:
            kwargs = dict(
                embed=embed,
                allowed_mentions=discord.AllowedMentions(everyone=False, roles=False, users=True),
            )
            if team_icon_file:
                kwargs["file"] = team_icon_file
            msg = await channel.send(**kwargs)

            # React with the team emoji (best-effort).
            if team_emoji:
                try:
                    await msg.add_reaction(team_emoji)
                except discord.DiscordException:
                    pass
        except discord.Forbidden:
            await interaction.response.send_message(
                "I don't have permission to post in the Hall of Fame channel.",
                ephemeral=True,
            )
            return

        await interaction.response.send_message(f"Posted in <#{channel.id}>.", ephemeral=True)

    async def on_error(self, interaction: discord.Interaction, error: Exception) -> None:
        print("HallOfFameModal error:", repr(error))
        msg = "Something went wrong while creating the Hall of Fame embed."
        try:
            if interaction.response.is_done():
                await interaction.followup.send(msg, ephemeral=True)
            else:
                await interaction.response.send_message(msg, ephemeral=True)
        except discord.DiscordException:
            pass


class FRTHallOfFameModal(discord.ui.Modal):
    """Hall of Fame modal for FRT: adds Mode field, Mode and Bracket inline."""

    edition_number = discord.ui.TextInput(
        label="Edition number",
        placeholder="e.g. 1",
        required=True,
        max_length=10,
    )
    team_name = discord.ui.TextInput(
        label="Team name",
        placeholder="e.g. OVERDOZEE",
        required=True,
        max_length=60,
    )
    bracket_url = discord.ui.TextInput(
        label="Bracket URL",
        placeholder="https://battlefy.com/...",
        required=True,
        max_length=200,
    )
    mode = discord.ui.TextInput(
        label="Mode",
        placeholder="e.g. Rondo (4v4)",
        required=True,
        max_length=80,
    )
    roster = discord.ui.TextInput(
        label="Roster (one per line: country + discord id)",
        placeholder="FR 123456789012345678\nMA <@123456789012345678>\n:flag_es: 123456789012345678",
        style=discord.TextStyle.paragraph,
        required=True,
        max_length=600,
    )

    def __init__(self) -> None:
        super().__init__(title="FRT Hall of Fame")

    async def on_submit(self, interaction: discord.Interaction):
        if not interaction.guild:
            await interaction.response.send_message("Run this in the server.", ephemeral=True)
            return

        server = config.server_for_guild_id(interaction.guild.id)
        if server is None:
            await interaction.response.send_message(
                "This server is not configured in `config.yaml` (missing matching `SERVER_ID`).",
                ephemeral=True,
            )
            return

        ttype = "FRT"
        hof_channel_id = _hall_of_fame_channel_id(server, tournament_type=ttype)
        if not hof_channel_id:
            await interaction.response.send_message(
                f"This server is missing `HALL_OF_FAME_CHANNEL_ID.{ttype}` in `config.yaml`.",
                ephemeral=True,
            )
            return

        ed_raw = (self.edition_number.value or "").strip()
        m = re.search(r"\d+", ed_raw)
        if not m:
            await interaction.response.send_message("Edition number must contain a number (e.g. `1`).", ephemeral=True)
            return
        edition = int(m.group(0))

        team = " ".join((self.team_name.value or "").strip().split())
        url = (self.bracket_url.value or "").strip()
        mode_val = (self.mode.value or "").strip() or "-"

        roster_lines, roster_err = _parse_roster(self.roster.value or "")
        if roster_err:
            await interaction.response.send_message(
                "Roster format (one per line):\n"
                "`FR 123456789012345678`\n"
                "`:flag_fr: <@123456789012345678>`\n"
                "`🇫🇷 123456789012345678`\n\n"
                f"{roster_err}",
                ephemeral=True,
            )
            return

        color = (server.embed_color or {}).get(ttype, 0xbe629b)
        team_icon_path = find_team_icon(team)
        team_icon_file = None
        team_emoji = await _ensure_team_emoji(interaction.guild, team)

        title = f"{ttype} #{edition} Champions — {team}{(' ' + team_emoji) if team_emoji else ''}"
        embed = discord.Embed(title=title, color=int(color))
        embed.add_field(name="Mode", value=mode_val, inline=True)
        embed.add_field(name="Bracket", value=f"[Battlefy]({url})" if url else "-", inline=True)
        embed.add_field(name="Roster", value="\n".join(roster_lines) or "-", inline=False)

        if team_icon_path:
            filename = team_icon_path.name
            team_icon_file = discord.File(team_icon_path, filename=filename)
            embed.set_image(url=f"attachment://{filename}")

        channel = interaction.guild.get_channel(hof_channel_id)
        if channel is None:
            try:
                channel = await interaction.guild.fetch_channel(hof_channel_id)
            except discord.DiscordException:
                channel = None

        if channel is None or not hasattr(channel, "send"):
            await interaction.response.send_message(
                f"Couldn't find the Hall of Fame channel. Check `HALL_OF_FAME_CHANNEL_ID.{ttype}` in `config.yaml`.",
                ephemeral=True,
            )
            return

        try:
            kwargs = dict(
                embed=embed,
                allowed_mentions=discord.AllowedMentions(everyone=False, roles=False, users=True),
            )
            if team_icon_file:
                kwargs["file"] = team_icon_file
            msg = await channel.send(**kwargs)
            if team_emoji:
                try:
                    await msg.add_reaction(team_emoji)
                except discord.DiscordException:
                    pass
        except discord.Forbidden:
            await interaction.response.send_message(
                "I don't have permission to post in the Hall of Fame channel.",
                ephemeral=True,
            )
            return

        await interaction.response.send_message(f"Posted in <#{channel.id}>.", ephemeral=True)

    async def on_error(self, interaction: discord.Interaction, error: Exception) -> None:
        print("FRTHallOfFameModal error:", repr(error))
        msg = "Something went wrong while creating the Hall of Fame embed."
        try:
            if interaction.response.is_done():
                await interaction.followup.send(msg, ephemeral=True)
            else:
                await interaction.response.send_message(msg, ephemeral=True)
        except discord.DiscordException:
            pass


class HallOfFameTypeSelect(discord.ui.Select):
    def __init__(self, *, options: list[discord.SelectOption]):
        super().__init__(
            placeholder="Select tournament type…",
            min_values=1,
            max_values=1,
            options=options,
        )

    async def callback(self, interaction: discord.Interaction):
        ttype = (self.values[0] if self.values else "").strip().upper()
        if ttype == "FRT":
            await interaction.response.send_modal(FRTHallOfFameModal())
        else:
            await interaction.response.send_modal(HallOfFameModal(tournament_type=ttype))


class HallOfFameTypeView(discord.ui.View):
    def __init__(self, *, options: list[discord.SelectOption]):
        super().__init__(timeout=120)
        self.add_item(HallOfFameTypeSelect(options=options))


class SponsorsModal(discord.ui.Modal):
    edition_number = discord.ui.TextInput(
        label="Edition number",
        placeholder="e.g. 8",
        required=True,
        max_length=10,
    )
    section_name = discord.ui.TextInput(
        label="Section name",
        placeholder="e.g. TRIAL",
        required=True,
        max_length=50,
    )
    sponsors = discord.ui.TextInput(
        label="Sponsors (one per line)",
        placeholder="Orion Esports | Morocco | 263329265594925057\n25€ | Other Team | France | 123456789",
        style=discord.TextStyle.paragraph,
        required=True,
        max_length=1200,
    )

    def __init__(self, *, tournament_type: str):
        self.tournament_type = (tournament_type or "").strip().upper()
        super().__init__(title=f"{self.tournament_type} Sponsors")

    async def on_submit(self, interaction: discord.Interaction):
        if not interaction.guild:
            await interaction.response.send_message("Run this in the server.", ephemeral=True)
            return

        server = config.server_for_guild_id(interaction.guild.id)
        if server is None:
            await interaction.response.send_message(
                "This server is not configured in `config.yaml` (missing matching `SERVER_ID`).",
                ephemeral=True,
            )
            return

        ttype = self.tournament_type or "PRT"
        channel_id = (server.sponsors_channel_id or {}).get(ttype)
        if not channel_id:
            await interaction.response.send_message(
                f"This server is missing `SPONSORS_CHANNEL_ID.{ttype}` in `config.yaml`.",
                ephemeral=True,
            )
            return

        # Parse edition
        ed_raw = (self.edition_number.value or "").strip()
        m = re.search(r"\d+", ed_raw)
        if not m:
            await interaction.response.send_message("Edition number must contain a number (e.g. `8`).", ephemeral=True)
            return
        edition = int(m.group(0))

        # Parse sponsor lines
        lines_in = (self.sponsors.value or "").splitlines()
        out_lines: list[str] = []
        for ln in lines_in:
            if not ln.strip():
                continue
            parsed = _parse_sponsor_line(ln)
            if parsed[0] is None:
                await interaction.response.send_message(f"Sponsor line error: {parsed[1]}", ephemeral=True)
                return
            team, flag, mention, amount = parsed  # type: ignore[misc]

            team_emoji = await _ensure_team_emoji(interaction.guild, team)
            out_lines.append(f"{amount} — {team_emoji + ' ' if team_emoji else ''}{flag} {mention}")

        if not out_lines:
            await interaction.response.send_message("Sponsors list is required (at least 1 line).", ephemeral=True)
            return

        section = " ".join((self.section_name.value or "").strip().split())
        if not section:
            section = "Sponsors"

        color = (server.embed_color or {}).get(ttype, 0xbe629b)
        embed = discord.Embed(title=f"{ttype} #{edition} Sponsors", color=int(color))
        embed.add_field(name=section, value="\n".join(out_lines), inline=False)
        embed.set_footer(text="Huge thanks for the support!")

        channel = interaction.guild.get_channel(channel_id)
        if channel is None:
            try:
                channel = await interaction.guild.fetch_channel(channel_id)
            except discord.DiscordException:
                channel = None

        if channel is None or not hasattr(channel, "send"):
            await interaction.response.send_message(
                f"Couldn't find the sponsors channel. Check `SPONSORS_CHANNEL_ID.{ttype}` in `config.yaml`.",
                ephemeral=True,
            )
            return

        try:
            msg = await channel.send(
                embed=embed,
                allowed_mentions=discord.AllowedMentions(everyone=False, roles=False, users=True),
            )
            # React with :heart_hands: (🫶). Best-effort.
            try:
                await msg.add_reaction("🫶")
            except discord.DiscordException:
                pass
        except discord.Forbidden:
            await interaction.response.send_message(
                "I don't have permission to post in the sponsors channel.",
                ephemeral=True,
            )
            return

        await interaction.response.send_message(f"Posted in <#{channel.id}>.", ephemeral=True)

    async def on_error(self, interaction: discord.Interaction, error: Exception) -> None:
        print("SponsorsModal error:", repr(error))
        msg = "Something went wrong while creating the sponsors embed."
        try:
            if interaction.response.is_done():
                await interaction.followup.send(msg, ephemeral=True)
            else:
                await interaction.response.send_message(msg, ephemeral=True)
        except discord.DiscordException:
            pass


class SponsorsTypeSelect(discord.ui.Select):
    def __init__(self, *, options: list[discord.SelectOption]):
        super().__init__(
            placeholder="Select tournament type…",
            min_values=1,
            max_values=1,
            options=options,
        )

    async def callback(self, interaction: discord.Interaction):
        ttype = (self.values[0] if self.values else "").strip().upper()
        await interaction.response.send_modal(SponsorsModal(tournament_type=ttype))


class SponsorsTypeView(discord.ui.View):
    def __init__(self, *, options: list[discord.SelectOption]):
        super().__init__(timeout=120)
        self.add_item(SponsorsTypeSelect(options=options))


class LeaderboardModal(discord.ui.Modal):
    date_range = discord.ui.TextInput(
        label="Date range",
        placeholder="23/02 -> 08/03",
        required=True,
        max_length=40,
    )

    def __init__(self) -> None:
        super().__init__(title="Leaderboard")

    async def on_submit(self, interaction: discord.Interaction):
        if not interaction.guild or not interaction.channel:
            await interaction.response.send_message("Run this in the server.", ephemeral=True)
            return

        # Normalize date range (replace -> with →, collapse spaces).
        raw = (self.date_range.value or "").strip()
        date_range = " ".join(raw.replace("->", "→").split())

        try:
            await interaction.response.defer(ephemeral=True, thinking=True)
        except discord.NotFound:
            return

        if not config.is_allowed_setup_channel(guild_id=interaction.guild.id, channel_id=interaction.channel.id):
            server = config.server_for_guild_id(interaction.guild.id)
            required = server.setup_channel_id if server else None
            if required is not None:
                await interaction.followup.send(f"Use this in <#{required}>.", ephemeral=True)
                return

        if not interaction.user.guild_permissions.administrator:
            await interaction.followup.send("Admins only.", ephemeral=True)
            return

        if not _LEADERBOARD_CSV.exists():
            await interaction.followup.send(
                "Couldn't find `csv_points/leaderboard.csv`.\n"
                "Generate it first by running: `python leaderboard.py`",
                ephemeral=True,
            )
            return

        import csv

        with _LEADERBOARD_CSV.open("r", newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames is None:
                await interaction.followup.send("Leaderboard CSV has no header row.", ephemeral=True)
                return

            required_cols = {"Rank", "Team", "Points"}
            missing = [c for c in required_cols if c not in set(reader.fieldnames)]
            if missing:
                await interaction.followup.send(
                    f"Leaderboard CSV missing columns: {', '.join(missing)}",
                    ephemeral=True,
                )
                return

            rows = list(reader)

        def _points_key(r: dict[str, str]) -> int:
            try:
                return int(round(float((r.get("Points") or '').strip() or "0")))
            except ValueError:
                return 0

        top = sorted(rows, key=lambda r: (-_points_key(r), (r.get("Team") or "").casefold()))[:48]
        embed = _format_leaderboard_embed(top, date_range=date_range)

        server = config.server_for_guild_id(interaction.guild.id)
        if server is None:
            await interaction.followup.send(
                "This server is not configured in `config.yaml` (missing matching `SERVER_ID`).",
                ephemeral=True,
            )
            return

        leaderboard_channel_id = _leaderboard_channel_id(server)
        if not leaderboard_channel_id:
            await interaction.followup.send(
                "This server is missing `LEADERBOARD_CHANNEL_ID` in `config.yaml`.",
                ephemeral=True,
            )
            return

        await _send_leaderboard_embed(
            interaction,
            embed=embed,
            leaderboard_channel_id=int(leaderboard_channel_id),
            ping_id=server.tournaments_ping_id,
            preview_label="Leaderboard",
            test_channel_id=server.test_channel_id,
            require_preview=True,
        )


class PartLeaderboardModal(discord.ui.Modal):
    date_range = discord.ui.TextInput(
        label="Date range",
        placeholder="23/02 -> 08/03",
        required=True,
        max_length=40,
    )

    def __init__(self, *, tournament_type: str):
        self.tournament_type = (tournament_type or "").strip().upper()
        super().__init__(title=f"{self.tournament_type or 'PRT'} Leaderboard")

    async def on_submit(self, interaction: discord.Interaction):
        if not interaction.guild or not interaction.channel:
            await interaction.response.send_message("Run this in the server.", ephemeral=True)
            return

        raw = (self.date_range.value or "").strip()
        date_range = " ".join(raw.replace("->", "→").split())

        try:
            await interaction.response.defer(ephemeral=True, thinking=True)
        except discord.NotFound:
            return

        if not config.is_allowed_setup_channel(guild_id=interaction.guild.id, channel_id=interaction.channel.id):
            server = config.server_for_guild_id(interaction.guild.id)
            required = server.setup_channel_id if server else None
            if required is not None:
                await interaction.followup.send(f"Use this in <#{required}>.", ephemeral=True)
                return

        if not interaction.user.guild_permissions.administrator:
            await interaction.followup.send("Admins only.", ephemeral=True)
            return

        server = config.server_for_guild_id(interaction.guild.id)
        if server is None:
            await interaction.followup.send(
                "This server is not configured in `config.yaml` (missing matching `SERVER_ID`).",
                ephemeral=True,
            )
            return

        ttype = self.tournament_type or "PRT"
        try:
            rows = _load_part_leaderboard_rows(_part_leaderboard_input_dir(ttype))
        except ValueError as e:
            await interaction.followup.send(str(e), ephemeral=True)
            return

        if not rows:
            await interaction.followup.send("No valid teams found in the PART leaderboard CSVs.", ephemeral=True)
            return

        top = sorted(rows, key=lambda r: (-int(r.get("Points", "0") or "0"), (r.get("Team") or "").casefold()))[:48]
        embed = _format_leaderboard_embed(top, date_range=date_range)

        leaderboard_channel_id = _leaderboard_channel_id(server, tournament_type=ttype)
        if not leaderboard_channel_id:
            await interaction.followup.send(
                f"This server is missing `LEADERBOARD_CHANNEL_ID.{ttype}` in `config.yaml`.",
                ephemeral=True,
            )
            return

        await _send_leaderboard_embed(
            interaction,
            embed=embed,
            leaderboard_channel_id=int(leaderboard_channel_id),
            ping_id=server.tournaments_ping_id,
            preview_label=f"{ttype} Leaderboard",
            test_channel_id=server.test_channel_id,
            require_preview=False,
        )

    async def on_error(self, interaction: discord.Interaction, error: Exception) -> None:
        print("PartLeaderboardModal error:", repr(error))
        msg = "Something went wrong while creating the leaderboard."
        try:
            if interaction.response.is_done():
                await interaction.followup.send(msg, ephemeral=True)
            else:
                await interaction.response.send_message(msg, ephemeral=True)
        except discord.DiscordException:
            pass


class LeaderboardTypeSelect(discord.ui.Select):
    def __init__(self, *, options: list[discord.SelectOption]):
        super().__init__(
            placeholder="Select tournament type…",
            min_values=1,
            max_values=1,
            options=options,
        )

    async def callback(self, interaction: discord.Interaction):
        ttype = (self.values[0] if self.values else "").strip().upper()
        await interaction.response.send_modal(PartLeaderboardModal(tournament_type=ttype))


class LeaderboardTypeView(discord.ui.View):
    def __init__(self, *, options: list[discord.SelectOption]):
        super().__init__(timeout=120)
        self.add_item(LeaderboardTypeSelect(options=options))


class SetupView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="🏆 Tournament Results",
        style=discord.ButtonStyle.primary,
        custom_id="rematchhq:tournament_results",
    )
    async def tournament_results(self, interaction: discord.Interaction, _: discord.ui.Button):
        if not interaction.guild or not interaction.channel:
            await interaction.response.send_message("Run this in the server.", ephemeral=True)
            return

        if not config.is_allowed_setup_channel(guild_id=interaction.guild.id, channel_id=interaction.channel.id):
            server = config.server_for_guild_id(interaction.guild.id)
            required = server.setup_channel_id if server else None
            if required is not None:
                await interaction.response.send_message(f"Use this in <#{required}>.", ephemeral=True)
                return

        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("Admins only.", ephemeral=True)
            return

        await interaction.response.send_modal(TournamentResultsModal())

    @discord.ui.button(
        label="📅 Tournament Today",
        style=discord.ButtonStyle.primary,
        custom_id="rematchhq:tournament_today",
    )
    async def tournament_today(self, interaction: discord.Interaction, _: discord.ui.Button):
        if not interaction.guild or not interaction.channel:
            await interaction.response.send_message("Run this in the server.", ephemeral=True)
            return

        if not config.is_allowed_setup_channel(guild_id=interaction.guild.id, channel_id=interaction.channel.id):
            server = config.server_for_guild_id(interaction.guild.id)
            required = server.setup_channel_id if server else None
            if required is not None:
                await interaction.response.send_message(f"Use this in <#{required}>.", ephemeral=True)
                return

        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("Admins only.", ephemeral=True)
            return

        if not config.NOTION_TOKEN or not config.NOTION_DATABASE_ID:
            await interaction.response.send_message(
                "Missing `NOTION_TOKEN` or `NOTION_DATABASE_ID` in `.env`.",
                ephemeral=True,
            )
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        print("Notion: querying today's tournaments...")
        try:
            client = NotionClient(config.NOTION_TOKEN)
            db = await client.retrieve_database(config.NOTION_DATABASE_ID)
            props = detect_props(db)
            payload = notion_query_payload_for_today_cups(props)
            pages = await client.query_database(config.NOTION_DATABASE_ID, payload)
        except httpx.ReadTimeout:
            print("Notion: ReadTimeout while querying database.")
            await interaction.followup.send(
                "Notion timed out while fetching tournaments. Try again in a bit.",
                ephemeral=True,
            )
            return
        except httpx.HTTPStatusError as e:
            print("Notion: HTTP error:", e.response.status_code, e.response.text[:500])
            await interaction.followup.send(
                f"Notion API error ({e.response.status_code}). Check `NOTION_TOKEN` and `NOTION_DATABASE_ID`.",
                ephemeral=True,
            )
            return
        except Exception as e:
            print("Notion: unexpected error:", repr(e))
            await interaction.followup.send("Notion error. Check terminal logs.", ephemeral=True)
            return

        tournaments = [t for p in pages if (t := extract_tournament(p, props))]
        tday = today_cet()
        tournaments_today = [t for t in tournaments if cet_day(t.starts_at) == tday]
        print(f"Notion: {len(tournaments_today)} tournament(s) today")

        if not tournaments_today:
            await interaction.followup.send("No tournaments found for today.", ephemeral=True)
            return

        items: list[tuple[discord.Embed, str, str | None, str | None]] = []
        for t in tournaments_today[:25]:
            entry = f"{t.entry_fee_eur:g}€" if isinstance(t.entry_fee_eur, (int, float)) else "-"
            prize = f"{t.prize_pool_eur:g}€" if isinstance(t.prize_pool_eur, (int, float)) else "-"
            fmt = (t.format or "").strip() or "-"

            website = f"[URL]({t.website_url})" if t.website_url else "-"
            dsc = f"[URL]({t.discord_url})" if t.discord_url else "-"

            org = (t.organization or "").strip()
            org_emoji = emoji_for_org(org, interaction.guild)
            title = f"{org_emoji} {t.title}".strip() if org_emoji else t.title

            e = discord.Embed(title=title, color=0xbe629b)
            # Row 1
            e.add_field(name="Format", value=fmt, inline=True)
            e.add_field(name="Entry fee", value=entry, inline=True)
            e.add_field(name="Prize pool", value=prize, inline=True)
            # Row 2
            e.add_field(name="Time", value=discord_timestamp(t.starts_at), inline=True)
            e.add_field(name="Website", value=website, inline=True)
            e.add_field(name="Discord", value=dsc, inline=True)

            icon_path = find_icon(org)
            icon_filename = icon_path.name if icon_path else None
            if icon_filename:
                e.set_thumbnail(url=f"attachment://{icon_filename}")
            items.append((e, org, str(icon_path) if icon_path else None, icon_filename))

        server = config.server_for_guild_id(interaction.guild.id)
        upcoming_channel_id = server.upcoming_tournaments_channel_id if server else None
        if not upcoming_channel_id:
            await interaction.followup.send(
                "This server is missing `UPCOMING_TOURNAMENTS_CHANNEL_ID` in `config.yaml`.",
                ephemeral=True,
            )
            return

        channel = interaction.guild.get_channel(upcoming_channel_id)
        if channel is None:
            try:
                channel = await interaction.guild.fetch_channel(upcoming_channel_id)
            except discord.DiscordException:
                channel = None

        if channel is None or not hasattr(channel, "send"):
            await interaction.followup.send(
                "Couldn't find tournaments channel. Check `UPCOMING_TOURNAMENTS_CHANNEL_ID` in `config.yaml`.",
                ephemeral=True,
            )
            return

        test_channel_id = server.test_channel_id if server else None
        if not test_channel_id:
            await interaction.followup.send(
                "This server is missing `TEST_CHANNEL_ID` in `config.yaml` (needed for previews).",
                ephemeral=True,
            )
            return
        test_channel = await _get_sendable_channel(interaction.guild, int(test_channel_id))
        if test_channel is None:
            await interaction.followup.send("Couldn't find the test channel.", ephemeral=True)
            return

        ping_id = server.tournaments_ping_id if server else None
        ping = None
        if ping_id:
            role = interaction.guild.get_role(ping_id)
            ping = f"<@&{ping_id}>" if role else f"<@{ping_id}>"

        async def _send_chunks(dest, *, preview: bool) -> list[int]:
            ids: list[int] = []
            for i in range(0, len(items), 10):
                chunk = items[i : i + 10]
                embeds = [e for (e, _, __, ___) in chunk]

                files: list[discord.File] = []
                seen: set[str] = set()
                for _, __org, path, filename in chunk:
                    if not path or not filename or filename in seen:
                        continue
                    seen.add(filename)
                    files.append(discord.File(path, filename=filename))

                if preview:
                    content = None
                    if i == 0:
                        content = f"[PREVIEW] Tournament Today\n{ping or ''}".strip()
                    msg = await dest.send(
                        content=content,
                        embeds=embeds,
                        files=files,
                        allowed_mentions=discord.AllowedMentions(everyone=False, roles=False, users=False),
                    )
                else:
                    msg = await dest.send(
                        content=ping if (ping and i == 0) else None,
                        embeds=embeds,
                        files=files,
                        allowed_mentions=discord.AllowedMentions(everyone=False, roles=True, users=True),
                    )

                    # React with tournament (org) emoji(s). Best-effort.
                    org_emojis = []
                    for __e, org, __p, __f in chunk:
                        em = emoji_for_org(org, interaction.guild)
                        if em and em not in org_emojis:
                            org_emojis.append(em)
                    for em in org_emojis[:5]:
                        try:
                            await msg.add_reaction(em)
                        except discord.DiscordException:
                            pass

                    # Publish in announcement channel so it cross-posts to followers.
                    try:
                        await msg.publish()
                    except discord.DiscordException:
                        pass

                ids.append(msg.id)
            return ids

        try:
            preview_ids = await _send_chunks(test_channel, preview=True)
        except discord.Forbidden:
            await interaction.followup.send("I don't have permission to post in the test channel.", ephemeral=True)
            return

        async def _publish(confirm_interaction: discord.Interaction) -> str:
            guild = confirm_interaction.guild
            if guild is None:
                return "Run this in the server."
            dest = await _get_sendable_channel(guild, int(upcoming_channel_id))
            if dest is None:
                return "Couldn't find the upcoming tournaments channel."
            try:
                await _send_chunks(dest, preview=False)
            except discord.Forbidden:
                return "I don't have permission to post in the tournaments channel."
            return f"Posted {len(items)} tournaments in <#{upcoming_channel_id}>."

        await interaction.followup.send(
            f"Preview posted in <#{test_channel_id}>. Confirm to post in <#{upcoming_channel_id}>.",
            ephemeral=True,
            view=ConfirmPostView(
                requester_id=interaction.user.id,
                test_channel_id=int(test_channel_id),
                preview_message_ids=preview_ids,
                publish_fn=_publish,
            ),
        )

    @discord.ui.button(
        label="📊 Leaderboard",
        style=discord.ButtonStyle.green,
        custom_id="rematchhq:leaderboard",
    )
    async def leaderboard(self, interaction: discord.Interaction, _: discord.ui.Button):
        if not interaction.guild or not interaction.channel:
            await interaction.response.send_message("Run this in the server.", ephemeral=True)
            return

        if not config.is_allowed_setup_channel(guild_id=interaction.guild.id, channel_id=interaction.channel.id):
            server = config.server_for_guild_id(interaction.guild.id)
            required = server.setup_channel_id if server else None
            if required is not None:
                await interaction.response.send_message(f"Use this in <#{required}>.", ephemeral=True)
                return

        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("Admins only.", ephemeral=True)
            return

        await interaction.response.send_modal(LeaderboardModal())

    @discord.ui.button(
        label="👑 Rosters",
        style=discord.ButtonStyle.green,
        custom_id="rematchhq:rosters_embeds",
    )
    async def rosters_embeds(self, interaction: discord.Interaction, _: discord.ui.Button):
        if not interaction.guild or not interaction.channel:
            await interaction.response.send_message("Run this in the server.", ephemeral=True)
            return

        try:
            await interaction.response.defer(ephemeral=True, thinking=True)
        except discord.NotFound:
            return

        if not config.is_allowed_setup_channel(guild_id=interaction.guild.id, channel_id=interaction.channel.id):
            server = config.server_for_guild_id(interaction.guild.id)
            required = server.setup_channel_id if server else None
            if required is not None:
                await interaction.followup.send(f"Use this in <#{required}>.", ephemeral=True)
                return

        if not interaction.user.guild_permissions.administrator:
            await interaction.followup.send("Admins only.", ephemeral=True)
            return

        server = config.server_for_guild_id(interaction.guild.id)
        rosters_channel_id = server.rosters_channel_id if server else None
        if not rosters_channel_id:
            await interaction.followup.send(
                "This server is missing `ROSTERS_CHANNEL_ID` in `config.yaml`.",
                ephemeral=True,
            )
            return

        if not _ROSTERS_YAML.exists():
            await interaction.followup.send(
                "Couldn't find `leaderboard/output/rosters.yaml`.",
                ephemeral=True,
            )
            return

        # Load rosters.yaml as: {team_name: {color: 0x..., roster: [{Country: id}, ...]}}
        with _ROSTERS_YAML.open("r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
        if not isinstance(raw, dict) or not raw:
            await interaction.followup.send("`leaderboard/output/rosters.yaml` is empty or invalid.", ephemeral=True)
            return

        test_channel_id = server.test_channel_id if server else None
        if not test_channel_id:
            await interaction.followup.send(
                "This server is missing `TEST_CHANNEL_ID` in `config.yaml` (needed for previews).",
                ephemeral=True,
            )
            return
        test_channel = await _get_sendable_channel(interaction.guild, int(test_channel_id))
        if test_channel is None:
            await interaction.followup.send("Couldn't find the test channel.", ephemeral=True)
            return

        ping_id = server.tournaments_ping_id if server else None
        ping = None
        if ping_id:
            ping_role = interaction.guild.get_role(ping_id)
            ping = f"<@&{ping_id}>" if ping_role else f"<@{ping_id}>"

        async def _build_payload(*, do_role_work: bool):
            embeds: list[discord.Embed] = []
            files: list[discord.File] = []
            reactions: list[str] = []
            added = 0

            assigned = 0
            already_had = 0
            missing_members = 0
            role_failures = 0
            roles_created = 0
            roles_renamed = 0
            roles_existing = 0
            member_cache: dict[int, discord.Member] = {}

            for idx, (team_name, team_block) in enumerate(raw.items(), start=1):
                if added >= 8:
                    break
                if not isinstance(team_name, str) or not team_name.strip():
                    continue
                if not isinstance(team_block, dict):
                    continue

                # Read colors list (for gradient) or fallback to single color
                colors_raw = team_block.get("colors")
                role_colors = None
                if colors_raw is not None:
                    if isinstance(colors_raw, list):
                        # Parse list of colors
                        parsed_colors = []
                        for color_val in colors_raw:
                            try:
                                parsed_colors.append(int(str(color_val).strip(), 0))
                            except (ValueError, TypeError):
                                pass
                        if parsed_colors:
                            role_colors = parsed_colors
                    else:
                        # Fallback: single color value (backward compatibility)
                        try:
                            role_colors = [int(str(colors_raw).strip(), 0)]
                        except ValueError:
                            role_colors = None
                
                # Also check for old "color" field for backward compatibility
                if role_colors is None:
                    color_raw = team_block.get("color")
                    if color_raw is not None:
                        try:
                            role_colors = [int(str(color_raw).strip(), 0)]
                        except ValueError:
                            role_colors = None

                players = team_block.get("roster")
                if not isinstance(players, list):
                    continue

                icon_path = find_team_icon(team_name)
                desired_role_name = f"#{idx} — {team_name}"

                role = None
                if do_role_work:
                    before_names = {r.name for r in interaction.guild.roles}
                    role = await _ensure_team_role(
                        interaction.guild,
                        role_name=desired_role_name,
                        team_name=team_name,
                        role_colors=role_colors,
                        position_offset=idx,
                    )
                    if role is None:
                        role_failures += 1
                    else:
                        if role.name not in before_names:
                            roles_created += 1
                        elif role.name == desired_role_name and team_name in before_names and desired_role_name not in before_names:
                            roles_renamed += 1
                        else:
                            roles_existing += 1
                else:
                    role = discord.utils.get(interaction.guild.roles, name=desired_role_name) or discord.utils.get(
                        interaction.guild.roles, name=team_name
                    )

                parsed_lines: list[str] = []
                for item in players:
                    if not isinstance(item, dict) or len(item) != 1:
                        continue
                    country, uid = next(iter(item.items()))
                    if not isinstance(country, str):
                        continue
                    try:
                        uid_i = int(uid)
                    except (TypeError, ValueError):
                        continue
                    flag = _country_to_flag(country) or country.strip()
                    parsed_lines.append(f"{flag} <@{uid_i}>")

                    if do_role_work and role is not None:
                        try:
                            member = member_cache.get(uid_i) or interaction.guild.get_member(uid_i)
                            if member is None:
                                member = await interaction.guild.fetch_member(uid_i)
                            member_cache[uid_i] = member
                            if role in getattr(member, "roles", []):
                                already_had += 1
                            else:
                                await member.add_roles(
                                    role,
                                    reason=f"Auto-assigned from rosters.yaml by {interaction.user} ({interaction.user.id})",
                                )
                                assigned += 1
                        except discord.NotFound:
                            missing_members += 1
                        except (discord.Forbidden, discord.HTTPException):
                            role_failures += 1

                if not parsed_lines:
                    continue

                team_emoji = emoji_for(team_name, interaction.guild)
                role_tag = role.mention if role is not None else team_name
                bits: list[str] = []
                if team_emoji:
                    bits.append(team_emoji)
                    if team_emoji not in reactions:
                        reactions.append(team_emoji)
                bits.append(role_tag)
                team_heading = "### " + " ".join(bits)

                # Optional Discord link for the team, shown under the heading and above the roster
                discord_url = team_block.get("discord")
                description_parts: list[str] = [team_heading]
                if isinstance(discord_url, str) and discord_url.strip():
                    description_parts.append(f"### <:Discord:1475149721369837721> [Discord Server]({discord_url.strip()})")
                description_parts.extend(parsed_lines)

                e = discord.Embed(
                    title=None,
                    color=0xbe629b,
                    description="\n".join(description_parts),
                )

                if icon_path:
                    try:
                        attach_name = f"{idx}_{icon_path.name}"
                        files.append(discord.File(icon_path, filename=attach_name))
                        e.set_image(url=f"attachment://{attach_name}")
                    except (OSError, discord.DiscordException):
                        pass

                embeds.append(e)
                added += 1

            summary = (
                f"Roles: **{roles_created}** created, **{roles_renamed}** renamed, **{roles_existing}** existing.\n"
                f"Assignments: **{assigned}** added, **{already_had}** already had, **{missing_members}** missing.\n"
                f"Failures (permissions/API): **{role_failures}**."
            )

            return embeds, files, reactions, added, summary

        embeds_preview, files_preview, __reactions, added_preview, __summary = await _build_payload(do_role_work=False)
        if added_preview == 0:
            await interaction.followup.send("No valid rosters found in `leaderboard/output/rosters.yaml`.", ephemeral=True)
            return

        try:
            preview_msg = await test_channel.send(
                content=f"[PREVIEW] Rosters\n{ping or ''}".strip(),
                embeds=embeds_preview,
                files=files_preview,
                allowed_mentions=discord.AllowedMentions(everyone=False, roles=False, users=False),
            )
        except discord.Forbidden:
            await interaction.followup.send("I don't have permission to post in the test channel.", ephemeral=True)
            return

        async def _publish(confirm_interaction: discord.Interaction) -> str:
            guild = confirm_interaction.guild
            if guild is None:
                return "Run this in the server."
            dest = await _get_sendable_channel(guild, int(rosters_channel_id))
            if dest is None:
                return "Couldn't find the rosters channel."

            embeds, files, reactions, added, summary = await _build_payload(do_role_work=True)
            if added == 0:
                return "No valid rosters found."
            msg = await dest.send(
                content=ping,
                embeds=embeds,
                files=files,
                allowed_mentions=discord.AllowedMentions(everyone=False, roles=True, users=True),
            )
            for em in reactions[:20]:
                try:
                    await msg.add_reaction(em)
                except discord.DiscordException:
                    pass
            return f"Posted rosters in <#{rosters_channel_id}>.\n{summary}"

        await interaction.followup.send(
            f"Preview posted in <#{test_channel_id}>. Confirm to post in <#{rosters_channel_id}>.",
            ephemeral=True,
            view=ConfirmPostView(
                requester_id=interaction.user.id,
                test_channel_id=int(test_channel_id),
                preview_message_ids=[preview_msg.id],
                publish_fn=_publish,
            ),
        )

    @discord.ui.button(
        label="💖 Compliment",
        style=discord.ButtonStyle.danger,
        custom_id="rematchhq:compliment",
        row=1,
    )
    async def compliment(self, interaction: discord.Interaction, _: discord.ui.Button):
        if not interaction.guild or not interaction.channel:
            await interaction.response.send_message("Run this in the server.", ephemeral=True)
            return

        if not config.is_allowed_setup_channel(guild_id=interaction.guild.id, channel_id=interaction.channel.id):
            server = config.server_for_guild_id(interaction.guild.id)
            required = server.setup_channel_id if server else None
            if required is not None:
                await interaction.response.send_message(f"Use this in <#{required}>.", ephemeral=True)
                return

        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("Admins only.", ephemeral=True)
            return

        server = config.server_for_guild_id(interaction.guild.id)
        compliments_channel_id = server.compliments_channel_id if server else None
        if not compliments_channel_id:
            await interaction.response.send_message(
                "This server is missing `COMPLIMENTS_CHANNEL_ID` in `config.yaml`.",
                ephemeral=True,
            )
            return

        test_channel_id = server.test_channel_id if server else None
        if not test_channel_id:
            await interaction.response.send_message(
                "This server is missing `TEST_CHANNEL_ID` in `config.yaml` (needed for previews).",
                ephemeral=True,
            )
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        guild = interaction.guild
        assert guild is not None

        compliments_ping_id = server.compliments_ping_id if server else None
        compliments_role = guild.get_role(compliments_ping_id) if compliments_ping_id else None

        members: list[discord.Member] = []
        try:
            async for m in guild.fetch_members(limit=None):
                if m.bot:
                    continue
                members.append(m)
        except discord.DiscordException:
            members = []
            for m in guild.members:
                if m.bot:
                    continue
                members.append(m)

        if not members:
            await interaction.followup.send("Couldn't find any non-bot members to compliment.", ephemeral=True)
            return

        test_channel = await _get_sendable_channel(guild, int(test_channel_id))
        if test_channel is None:
            await interaction.followup.send("Couldn't find the test channel.", ephemeral=True)
            return

        compliments_channel = await _get_sendable_channel(guild, int(compliments_channel_id))
        if compliments_channel is None:
            await interaction.followup.send("Couldn't find the compliments channel.", ephemeral=True)
            return

        if not hasattr(compliments_channel, "set_permissions") or not hasattr(compliments_channel, "overwrites"):
            await interaction.followup.send("The compliments channel does not support permission overwrites.", ephemeral=True)
            return

        chosen: discord.Member | None = None

        def _render_content(member: discord.Member) -> str:
            return (
                f"Hey **{member.mention}**, it's your turn for the **compliment of the day**! 🌟\n"
                "Pick a **rival player or a rival team** and say something positive about them."
            )

        async def _post_preview(*, exclude_member_id: int | None = None) -> tuple[list[int], str]:
            nonlocal chosen

            candidates = [m for m in members if exclude_member_id is None or m.id != exclude_member_id]
            if not candidates:
                candidates = list(members)
            if not candidates:
                raise RuntimeError("Couldn't find any non-bot members to compliment.")

            chosen = random.choice(candidates)

            preview_content = "[PREVIEW] Compliment\n" + _render_content(chosen)
            preview_msg = await test_channel.send(
                content=preview_content,
                allowed_mentions=discord.AllowedMentions(everyone=False, roles=False, users=False),
            )
            return [preview_msg.id], f"Preview posted in <#{int(test_channel_id)}> for {chosen.mention}."

        async def _publish(confirm_interaction: discord.Interaction) -> str:
            if chosen is None:
                return "No compliment target is selected."

            try:
                if compliments_role is not None:
                    for member in members:
                        if compliments_role in getattr(member, "roles", []):
                            await member.remove_roles(
                                compliments_role,
                                reason=f"Compliment channel now uses member permissions; updated by {confirm_interaction.user} ({confirm_interaction.user.id})",
                            )

                for target in list(compliments_channel.overwrites):
                    if not isinstance(target, discord.Member):
                        continue
                    await compliments_channel.set_permissions(
                        target,
                        overwrite=None,
                        reason=f"Cleared previous compliment channel member permissions by {confirm_interaction.user} ({confirm_interaction.user.id})",
                    )

                chosen_overwrite = compliments_channel.overwrites_for(chosen)
                chosen_overwrite.send_messages = True
                await compliments_channel.set_permissions(
                    chosen,
                    overwrite=chosen_overwrite,
                    reason=f"Compliment of the day assigned by {confirm_interaction.user} ({confirm_interaction.user.id})",
                )
            except (discord.Forbidden, discord.HTTPException):
                return "I couldn't update the compliments channel permissions. Check my channel permissions and role position."

            try:
                await compliments_channel.send(_render_content(chosen))
            except discord.DiscordException:
                return "Failed to send the compliment message."

            return f"Posted compliment of the day for {chosen.mention} in <#{int(compliments_channel_id)}>."

        try:
            preview_message_ids, preview_status = await _post_preview()
        except (RuntimeError, discord.DiscordException) as e:
            await interaction.followup.send(str(e), ephemeral=True)
            return

        async def _reroll(reroll_interaction: discord.Interaction) -> tuple[list[int], str]:
            try:
                return await _post_preview(exclude_member_id=chosen.id if chosen is not None and len(members) > 1 else None)
            except (RuntimeError, discord.DiscordException) as e:
                return [], str(e)

        await interaction.followup.send(
            preview_status + f" Confirm to post in <#{int(compliments_channel_id)}> or pick another person.",
            ephemeral=True,
            view=ComplimentPreviewView(
                requester_id=interaction.user.id,
                test_channel_id=int(test_channel_id),
                preview_message_ids=preview_message_ids,
                reroll_fn=_reroll,
                publish_fn=_publish,
            ),
        )

    @discord.ui.button(
        label="🗑️ Purge Scrims",
        style=discord.ButtonStyle.danger,
        custom_id="rematchhq:purge_scrims",
        row=1,
    )
    async def purge_scrims(self, interaction: discord.Interaction, _: discord.ui.Button):
        if not interaction.guild or not interaction.channel:
            await interaction.response.send_message("Run this in the server.", ephemeral=True)
            return

        if not config.is_allowed_setup_channel(guild_id=interaction.guild.id, channel_id=interaction.channel.id):
            server = config.server_for_guild_id(interaction.guild.id)
            required = server.setup_channel_id if server else None
            if required is not None:
                await interaction.response.send_message(f"Use this in <#{required}>.", ephemeral=True)
                return

        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("Admins only.", ephemeral=True)
            return

        server = config.server_for_guild_id(interaction.guild.id)
        forum_id = server.scrim_forum_channel_id if server else None
        exclude_uid = server.scrim_forum_user_id_exclude if server else None
        if not forum_id:
            await interaction.response.send_message(
                "Missing `SCRIM_FORUM_CHANNEL_ID` in `config.yaml` for this server.",
                ephemeral=True,
            )
            return

        await interaction.response.send_message(
            "You're about to delete **all posts** in the configured scrims forum.\n\n"
            "Press **Confirm purge** to proceed.",
            ephemeral=True,
            view=ForumPurgeConfirmView(
                requester_id=interaction.user.id,
                forum_channel_id=int(forum_id),
                exclude_user_id=(int(exclude_uid) if exclude_uid else None),
            ),
        )

    @discord.ui.button(
        label="🔮 Add Prediction",
        style=discord.ButtonStyle.secondary,
        custom_id="rematchhq:prediction",
        row=1,
    )
    async def prediction(self, interaction: discord.Interaction, _: discord.ui.Button):
        if not interaction.guild or not interaction.channel:
            await interaction.response.send_message("Run this in the server.", ephemeral=True)
            return

        if not config.is_allowed_setup_channel(guild_id=interaction.guild.id, channel_id=interaction.channel.id):
            server = config.server_for_guild_id(interaction.guild.id)
            required = server.setup_channel_id if server else None
            if required is not None:
                await interaction.response.send_message(f"Use this in <#{required}>.", ephemeral=True)
                return

        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("Admins only.", ephemeral=True)
            return

        await interaction.response.send_modal(PredictionPollModal())

    @discord.ui.button(
        label="📈 Calculate Predictions",
        style=discord.ButtonStyle.secondary,
        custom_id="rematchhq:calculate_predictions",
        row=1,
    )
    async def calculate_predictions(self, interaction: discord.Interaction, _: discord.ui.Button):
        if not interaction.guild or not interaction.channel:
            await interaction.response.send_message("Run this in the server.", ephemeral=True)
            return

        if not config.is_allowed_setup_channel(guild_id=interaction.guild.id, channel_id=interaction.channel.id):
            server = config.server_for_guild_id(interaction.guild.id)
            required = server.setup_channel_id if server else None
            if required is not None:
                await interaction.response.send_message(f"Use this in <#{required}>.", ephemeral=True)
                return

        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("Admins only.", ephemeral=True)
            return

        await interaction.response.send_modal(PredictionResultsModal())

    async def on_error(self, interaction: discord.Interaction, error: Exception, item) -> None:
        print("SetupView error:", repr(error))
        msg = "Something went wrong handling that button."
        try:
            if interaction.response.is_done():
                await interaction.followup.send(msg, ephemeral=True)
            else:
                await interaction.response.send_message(msg, ephemeral=True)
        except discord.DiscordException:
            # If the interaction already expired/was acknowledged, we can't respond.
            pass


async def _iter_archived_threads_best_effort(
    forum: discord.abc.GuildChannel,
    *,
    private: bool,
) -> list[discord.Thread]:
    """
    Best-effort archived thread fetch across discord.py versions.
    Returns a list (may be empty) and never raises.
    """
    threads: list[discord.Thread] = []

    archived = getattr(forum, "archived_threads", None)
    if not archived:
        return threads

    # discord.py signatures vary slightly across versions (private/joined flags, limit support).
    # We'll try a few compatible call shapes.
    call_variants = [
        lambda: archived(private=private, limit=None),
        lambda: archived(private=private),
        lambda: archived(limit=None),
        lambda: archived(),
    ]

    it = None
    for make in call_variants:
        try:
            it = make()
            break
        except TypeError:
            it = None
            continue
        except discord.DiscordException:
            return threads

    if it is None:
        return threads

    try:
        async for t in it:
            # If we couldn't pass private=..., filter here when possible.
            if private and hasattr(t, "is_private") and callable(getattr(t, "is_private")):
                try:
                    if not t.is_private():
                        continue
                except Exception:
                    pass
            threads.append(t)
    except discord.DiscordException:
        return threads

    return threads


async def _purge_forum_posts(
    interaction: discord.Interaction,
    forum: discord.ForumChannel,
    *,
    exclude_user_id: int | None,
) -> None:
    await interaction.response.defer(ephemeral=True, thinking=True)

    if not interaction.guild:
        await interaction.followup.send("Run this in the server.", ephemeral=True)
        return

    # Collect active threads (guild-wide API, then filter by forum parent)
    all_active: list[discord.Thread] = []
    try:
        all_active = list(await interaction.guild.active_threads())
    except discord.DiscordException:
        all_active = []

    candidates: dict[int, discord.Thread] = {t.id: t for t in all_active if getattr(t, "parent_id", None) == forum.id}

    # Add archived threads (public + private best-effort).
    for t in await _iter_archived_threads_best_effort(forum, private=False):
        if getattr(t, "parent_id", None) == forum.id:
            candidates.setdefault(t.id, t)
    for t in await _iter_archived_threads_best_effort(forum, private=True):
        if getattr(t, "parent_id", None) == forum.id:
            candidates.setdefault(t.id, t)

    threads = list(candidates.values())
    if not threads:
        await interaction.followup.send(f"No posts found in {forum.mention}.", ephemeral=True)
        return

    skipped = 0
    if exclude_user_id:
        filtered: list[discord.Thread] = []
        for t in threads:
            if getattr(t, "owner_id", None) == int(exclude_user_id):
                skipped += 1
                continue
            filtered.append(t)
        threads = filtered

    if not threads:
        await interaction.followup.send(
            f"Found posts in {forum.mention} but skipped **{skipped}** post(s) due to the exclude user id.",
            ephemeral=True,
        )
        return

    ok = 0
    failed = 0
    last_err: str | None = None

    for t in threads:
        try:
            await t.delete(reason=f"/setup purge requested by {interaction.user} ({interaction.user.id})")
            ok += 1
        except discord.Forbidden:
            failed += 1
            last_err = "Missing permissions to delete some threads (need Manage Threads / Manage Channels)."
        except discord.HTTPException as e:
            failed += 1
            last_err = f"HTTP error while deleting: {getattr(e, 'text', None) or repr(e)}"

    msg = f"Purged **{ok}** post(s) in {forum.mention}."
    if skipped:
        msg += f" Skipped: **{skipped}**."
    if failed:
        msg += f" Failed: **{failed}**."
    if last_err:
        msg += f"\n\nNote: {last_err}"
    await interaction.followup.send(msg, ephemeral=True)


class ForumPurgeConfirmView(discord.ui.View):
    def __init__(self, *, requester_id: int, forum_channel_id: int, exclude_user_id: int | None):
        super().__init__(timeout=180)
        self.requester_id = requester_id
        self.forum_channel_id = int(forum_channel_id)
        self.exclude_user_id = int(exclude_user_id) if exclude_user_id else None

    @discord.ui.button(label="Confirm purge", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, _: discord.ui.Button):
        if interaction.user.id != self.requester_id:
            await interaction.response.send_message("Only the admin who opened this can confirm.", ephemeral=True)
            return

        if not interaction.guild:
            await interaction.response.send_message("Run this in the server.", ephemeral=True)
            return

        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("Admins only.", ephemeral=True)
            return

        channel = interaction.guild.get_channel(self.forum_channel_id)
        if channel is None:
            try:
                channel = await interaction.guild.fetch_channel(self.forum_channel_id)
            except discord.DiscordException:
                channel = None

        if not isinstance(channel, discord.ForumChannel):
            await interaction.response.send_message("Couldn't find that forum channel.", ephemeral=True)
            return

        await _purge_forum_posts(interaction, channel, exclude_user_id=self.exclude_user_id)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, _: discord.ui.Button):
        if interaction.user.id != self.requester_id:
            await interaction.response.send_message("Only the admin who opened this can cancel.", ephemeral=True)
            return
        await interaction.response.send_message("Cancelled.", ephemeral=True)


class SetupPartView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="🏆 Tournament Info",
        style=discord.ButtonStyle.primary,
        custom_id="rematchhq:tournament_info",
    )
    async def tournament_info(self, interaction: discord.Interaction, _: discord.ui.Button):
        if not interaction.guild or not interaction.channel:
            await interaction.response.send_message("Run this in the server.", ephemeral=True)
            return

        if not config.is_allowed_setup_channel(guild_id=interaction.guild.id, channel_id=interaction.channel.id):
            server = config.server_for_guild_id(interaction.guild.id)
            required = server.setup_channel_id if server else None
            if required is not None:
                await interaction.response.send_message(f"Use this in <#{required}>.", ephemeral=True)
                return

        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("Admins only.", ephemeral=True)
            return

        server = config.server_for_guild_id(interaction.guild.id)
        if server is None:
            await interaction.response.send_message(
                "This server is not configured in `config.yaml` (missing matching `SERVER_ID`).",
                ephemeral=True,
            )
            return

        kinds = _pick_tournament_types(server, require_key="tournament_info_channel_id")

        options = [
            discord.SelectOption(
                label=k,
                value=k,
                description=f"Create a {k} tournament info embed",
            )
            for k in kinds
        ]
        await interaction.response.send_message(
            "Select which tournament you want to create the info embed for.",
            ephemeral=True,
            view=TournamentInfoTypeView(options=options),
        )

    @discord.ui.button(
        label="🥇 Hall of Fame",
        style=discord.ButtonStyle.red,
        custom_id="rematchhq:hall_of_fame",
    )
    async def hall_of_fame(self, interaction: discord.Interaction, _: discord.ui.Button):
        if not interaction.guild or not interaction.channel:
            await interaction.response.send_message("Run this in the server.", ephemeral=True)
            return

        if not config.is_allowed_setup_channel(guild_id=interaction.guild.id, channel_id=interaction.channel.id):
            server = config.server_for_guild_id(interaction.guild.id)
            required = server.setup_channel_id if server else None
            if required is not None:
                await interaction.response.send_message(f"Use this in <#{required}>.", ephemeral=True)
                return

        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("Admins only.", ephemeral=True)
            return

        server = config.server_for_guild_id(interaction.guild.id)
        if server is None:
            await interaction.response.send_message(
                "This server is not configured in `config.yaml` (missing matching `SERVER_ID`).",
                ephemeral=True,
            )
            return

        kinds = _pick_tournament_types(server, require_key="hall_of_fame_channel_id")
        options = [
            discord.SelectOption(
                label=k,
                value=k,
                description=f"Post {k} champions to Hall of Fame",
            )
            for k in kinds
        ]
        await interaction.response.send_message(
            "Select which tournament you want to post Hall of Fame for.",
            ephemeral=True,
            view=HallOfFameTypeView(options=options),
        )

    @discord.ui.button(
        label="📊 Leaderboard",
        style=discord.ButtonStyle.secondary,
        custom_id="rematchhq:leaderboard_part",
    )
    async def leaderboard_part(self, interaction: discord.Interaction, _: discord.ui.Button):
        if not interaction.guild or not interaction.channel:
            await interaction.response.send_message("Run this in the server.", ephemeral=True)
            return

        if not config.is_allowed_setup_channel(guild_id=interaction.guild.id, channel_id=interaction.channel.id):
            server = config.server_for_guild_id(interaction.guild.id)
            required = server.setup_channel_id if server else None
            if required is not None:
                await interaction.response.send_message(f"Use this in <#{required}>.", ephemeral=True)
                return

        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("Admins only.", ephemeral=True)
            return

        server = config.server_for_guild_id(interaction.guild.id)
        if server is None:
            await interaction.response.send_message(
                "This server is not configured in `config.yaml` (missing matching `SERVER_ID`).",
                ephemeral=True,
            )
            return

        kinds = _pick_tournament_types(server, require_key="leaderboard_channel_id")
        options = [
            discord.SelectOption(
                label=k,
                value=k,
                description=f"Post the {k} leaderboard",
            )
            for k in kinds
        ]
        await interaction.response.send_message(
            "Select which tournament leaderboard you want to post.",
            ephemeral=True,
            view=LeaderboardTypeView(options=options),
        )

    @discord.ui.button(
        label="💰 Sponsors",
        style=discord.ButtonStyle.green,
        custom_id="rematchhq:sponsors_prt",
    )
    async def sponsors_prt(self, interaction: discord.Interaction, _: discord.ui.Button):
        if not interaction.guild or not interaction.channel:
            await interaction.response.send_message("Run this in the server.", ephemeral=True)
            return

        if not config.is_allowed_setup_channel(guild_id=interaction.guild.id, channel_id=interaction.channel.id):
            server = config.server_for_guild_id(interaction.guild.id)
            required = server.setup_channel_id if server else None
            if required is not None:
                await interaction.response.send_message(f"Use this in <#{required}>.", ephemeral=True)
                return

        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("Admins only.", ephemeral=True)
            return

        server = config.server_for_guild_id(interaction.guild.id)
        if server is None:
            await interaction.response.send_message(
                "This server is not configured in `config.yaml` (missing matching `SERVER_ID`).",
                ephemeral=True,
            )
            return

        kinds = _pick_tournament_types(server, require_key="sponsors_channel_id")
        options = [
            discord.SelectOption(
                label=k,
                value=k,
                description=f"Create a {k} sponsors embed",
            )
            for k in kinds
        ]
        await interaction.response.send_message(
            "Select which tournament you want to create the sponsors embed for.",
            ephemeral=True,
            view=SponsorsTypeView(options=options),
        )


class AcademyRoleSelect(discord.ui.Select):
    def __init__(self):
        options = [
            discord.SelectOption(label=r, value=r, description=f"Register as {r}") for r in ROLES
        ]
        super().__init__(
            placeholder="Select your role…",
            min_values=1,
            max_values=len(ROLES),
            options=options,
        )

    async def callback(self, interaction: discord.Interaction):
        roles = [v.strip() for v in (self.values or []) if v.strip()]
        username = getattr(interaction.user, "name", "") or ""
        try:
            registered = await register_player(username=username, roles=roles, default_tier=3)
        except ValueError:
            await interaction.response.edit_message(
                content="Invalid role selection. Please try again.",
                view=None,
            )
            return

        parts = [f"**{r}** (tier **{t}**)" for r, t in registered.items()]
        await interaction.response.edit_message(
            content=f"Registered **{username}** as " + ", ".join(parts) + ".",
            view=None,
        )


class AcademyRoleView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=120)
        self.add_item(AcademyRoleSelect())


class AcademySetupView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="Register",
        style=discord.ButtonStyle.primary,
        custom_id="rematchhq:academy_register",
    )
    async def register(self, interaction: discord.Interaction, _: discord.ui.Button):
        await interaction.response.send_message(
            "What’s your role?",
            ephemeral=True,
            view=AcademyRoleView(),
        )

    @discord.ui.button(
        label="Unregister",
        style=discord.ButtonStyle.danger,
        custom_id="rematchhq:academy_unregister",
    )
    async def unregister(self, interaction: discord.Interaction, _: discord.ui.Button):
        username = getattr(interaction.user, "name", "") or ""
        existed = await unregister_player(username=username)
        msg = "You’ve been unregistered." if existed else "You weren’t registered."
        await interaction.response.send_message(msg, ephemeral=True)

    @discord.ui.button(
        label="Create teams",
        style=discord.ButtonStyle.secondary,
        custom_id="rematchhq:academy_create_teams",
    )
    async def create_teams(self, interaction: discord.Interaction, _: discord.ui.Button):
        if not interaction.guild:
            await interaction.response.send_message("Run this in the server.", ephemeral=True)
            return

        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("Admins only.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True, thinking=True)
        teams = await create_teams_from_file()

        if not teams:
            await interaction.followup.send(
                "Couldn't create any complete teams (need 1 player for each role, and 5 distinct players).",
                ephemeral=True,
            )
            return

        await interaction.followup.send(
            f"Generated **{len(teams)}** academy team(s) into `{TEAMS_YAML_PATH.name}`.",
            ephemeral=True,
        )

        # Preview ALL teams (chunked to stay under Discord's 2000-char limit).
        blocks: list[str] = []
        cur = ""
        for i, team in enumerate(teams, start=1):
            section_lines = [f"academy team {i}:"]
            for role in ROLES:
                u, t = team.get(role, ("-", 0))
                section_lines.append(f"  {role}: {u} ({t})")
            section = "\n".join(section_lines) + "\n"

            # Keep some headroom for code fences.
            if len(cur) + len(section) > 1800 and cur.strip():
                blocks.append(cur.rstrip())
                cur = ""
            cur += section
        if cur.strip():
            blocks.append(cur.rstrip())

        for b in blocks:
            await interaction.followup.send("```" + b + "```", ephemeral=True)

    async def on_error(self, interaction: discord.Interaction, error: Exception, item) -> None:
        print("AcademySetupView error:", repr(error))
        msg = "Something went wrong handling that action."
        try:
            if interaction.response.is_done():
                await interaction.followup.send(msg, ephemeral=True)
            else:
                await interaction.response.send_message(msg, ephemeral=True)
        except discord.DiscordException:
            pass
