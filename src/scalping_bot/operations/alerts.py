from __future__ import annotations

from dataclasses import dataclass

import httpx


class CriticalAlertFailure(RuntimeError):
    pass


@dataclass(frozen=True)
class TelegramNotifier:
    bot_token: str
    chat_id: str

    async def send(self, message: str, *, critical: bool = False) -> None:
        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                response = await client.post(url, json={"chat_id": self.chat_id, "text": message})
                response.raise_for_status()
        except (httpx.HTTPError, OSError) as error:
            if critical:
                raise CriticalAlertFailure(
                    "critical Telegram alert could not be delivered"
                ) from error

    async def heartbeat(self, *, mode: str, reconciliation_ok: bool) -> None:
        await self.send(
            f"scalping-bot daily heartbeat: mode={mode}, reconciliation_ok={reconciliation_ok}",
            critical=True,
        )
