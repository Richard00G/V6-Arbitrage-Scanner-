import asyncio
import json
from decimal import Decimal
import websockets


class BinanceFeed:
    """
    Feed de precio en tiempo real desde Binance.

    Utiliza el stream bookTicker para obtener:
        bid = mejor precio de venta disponible
        ask = mejor precio de compra disponible

    No ejecuta operaciones.
    """

    def __init__(self, symbol="BTCUSDT"):
        self.symbol = symbol.lower()
        self.url = (
            f"wss://stream.binance.com:9443/ws/"
            f"{self.symbol}@bookTicker"
        )

        self.bid = None
        self.ask = None
        self.bid_qty = None
        self.ask_qty = None
        self.timestamp = None

        self.connected = False

    async def connect(self):
        print("=" * 70)
        print("BINANCE REAL-TIME FEED")
        print("=" * 70)
        print(f"Symbol : {self.symbol.upper()}")
        print(f"Stream : bookTicker")
        print()

        while True:
            try:
                print("[BINANCE] Conectando...")

                async with websockets.connect(
                    self.url,
                    ping_interval=20,
                    ping_timeout=20,
                ) as ws:

                    self.connected = True

                    print(
                        "[BINANCE] ✅ WebSocket conectado"
                    )
                    print()

                    async for message in ws:
                        await self._process(message)

            except asyncio.CancelledError:
                raise

            except Exception as exc:
                self.connected = False

                print(
                    f"[BINANCE] ❌ "
                    f"{type(exc).__name__}: {exc}"
                )

                print(
                    "[BINANCE] Reintentando en 3 segundos..."
                )

                await asyncio.sleep(3)

    async def _process(self, message):
        data = json.loads(message)

        self.bid = Decimal(data["b"])
        self.ask = Decimal(data["a"])

        self.bid_qty = Decimal(data["B"])
        self.ask_qty = Decimal(data["A"])

        self.timestamp = (
            data.get("E")
        )

        mid = (
            self.bid + self.ask
        ) / Decimal("2")

        spread = (
            self.ask - self.bid
        )

        spread_pct = (
            spread / mid
        ) * Decimal("100")

        print(
            f"[BINANCE] "
            f"Bid={self.bid} "
            f"Ask={self.ask} "
            f"Mid={mid:.2f} "
            f"Spread={spread_pct:.5f}%"
        )


async def main():
    feed = BinanceFeed("BTCUSDT")

    try:
        await feed.connect()
    except KeyboardInterrupt:
        print()
        print("[BINANCE] Detenido.")


if __name__ == "__main__":
    asyncio.run(main())
