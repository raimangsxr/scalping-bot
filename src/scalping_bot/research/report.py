from __future__ import annotations

import json

from scalping_bot.domain.state import InMemoryState


def render_scalp_report(state: InMemoryState) -> str:
    lines = ["# Scalp trace", ""]
    for decision in state.audit:
        lines.extend(
            (
                f"## {decision.kind} — {decision.event_id}",
                "",
                "```json",
                json.dumps(decision.details, sort_keys=True, indent=2),
                "```",
                "",
            )
        )
    return "\n".join(lines)
