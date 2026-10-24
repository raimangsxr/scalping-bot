from dataclasses import dataclass
from decimal import Decimal

from scalping_bot.research.features import TradeFlowFeatures


@dataclass(frozen=True)
class TradeFlowConfirmation:
    min_delta: Decimal
    min_intensity: Decimal

    def allows(self, features: TradeFlowFeatures) -> bool:
        return (
            features.valid
            and features.cumulative_delta >= self.min_delta
            and features.trade_intensity >= self.min_intensity
        )
