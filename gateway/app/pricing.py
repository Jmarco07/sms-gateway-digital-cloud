"""Price lookup by destination prefix, and request validation helpers."""
import re
from dataclasses import dataclass

E164 = re.compile(r"^\+[1-9][0-9]{6,14}$")  # '+', no leading zero, 7-15 digits in total
# Sender id: up to 11 alphanumeric characters, or a phone number.
SENDER = re.compile(r"^(?:[A-Za-z0-9]{1,11}|\+?[0-9]{1,15})$")


@dataclass(frozen=True)
class PriceTable:
    """Longest-prefix match over the (small) price table."""

    entries: dict[str, tuple[str, int]]  # prefix -> (country, price_micros)

    def lookup(self, e164: str) -> tuple[str, int] | None:
        digits = e164.lstrip("+")
        for length in range(min(4, len(digits)), 0, -1):
            hit = self.entries.get(digits[:length])
            if hit:
                return hit
        return None


async def load_price_table(conn) -> PriceTable:
    rows = await conn.fetch("SELECT prefix, country, price_micros FROM prices")
    return PriceTable({r["prefix"]: (r["country"], r["price_micros"]) for r in rows})
