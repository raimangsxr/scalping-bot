from dataclasses import dataclass
from decimal import Decimal

from scalping_bot.research.features import BookFlowFeatures


@dataclass(frozen=True)
class ShadowDecision:
    hypothetical_allow: bool
    executable: bool = False
    reason: str = "Book Flow is restricted to Shadow Evaluation"


@dataclass(frozen=True)
class ShadowBookConfirmation:
    min_imbalance: Decimal

    def evaluate(self, features: BookFlowFeatures) -> ShadowDecision:
        allow = (
            features.valid
            and features.best_quote_imbalance >= self.min_imbalance
            and features.depth_imbalance >= self.min_imbalance
            and features.liquidity_change_confirmed_by_trades
        )
        return ShadowDecision(hypothetical_allow=allow)
