import asyncio
import json
import time
from decimal import Decimal
from dataclasses import dataclass
from datetime import datetime, timezone

import websockets

from rpc import create_web3, RPCRateLimiter
from dex.aerodrome import Aerodrome

from config import (
    USDC_ADDRESS,
    WETH_ADDRESS,
    CBBTC_ADDRESS,
)


# ============================================================
# CONFIG
# ============================================================

BINANCE_WS = "wss://stream.binance.com:9443/ws/btcusdt@bookTicker"

CAPITAL_USDC = Decimal("100")

AERO_INTERVAL = 1.0

# Solo mostrar oportunidades que superen esto
MIN_SPREAD_PCT = Decimal("0.10")


# ============================================================
# ESTADO DE MERCADO
# ============================================================

@dataclass
class BinancePrice:
    bid: Decimal = Decimal("0")
    ask: Decimal = Decimal("0")
    timestamp: float = 0.0


@dataclass
class AeroPrice:
    price: Decimal = Decimal("0")
    amount_cbBTC: Decimal = Decimal("0")
    timestamp: float = 0.0


binance = BinancePrice()
aero = AeroPrice()


# ============================================================
# HELPERS
# ============================================================

def now_str():

    return datetime.now(
        timezone.utc
    ).strftime(
        "%H:%M:%S.%f"
    )[:-3]


def spread_pct(
    low,
    high,
):

    if low <= 0:
        return Decimal("0")

    return (
        (high - low)
        / low
        * Decimal("100")
    )


# ============================================================
# BINANCE WEBSOCKET
# ============================================================

async def binance_feed():

    print()
    print("=" * 70)
    print("BINANCE WEBSOCKET")
    print("=" * 70)
    print(
        f"Stream: {BINANCE_WS}"
    )
    print()

    while True:

        try:

            async with websockets.connect(
                BINANCE_WS,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=5,
            ) as ws:

                print(
                    "[BINANCE] WebSocket conectado"
                )

                async for message in ws:

                    data = json.loads(
                        message
                    )

                    bid = Decimal(
                        data["b"]
                    )

                    ask = Decimal(
                        data["a"]
                    )

                    ts = time.time()

                    binance.bid = bid
                    binance.ask = ask
                    binance.timestamp = ts

        except Exception as exc:

            print(
                f"[BINANCE] "
                f"{type(exc).__name__}: "
                f"{exc}"
            )

            print(
                "[BINANCE] Reconectando en 2s..."
            )

            await asyncio.sleep(2)


# ============================================================
# AERODROME PRICE
# ============================================================

async def aerodrome_feed(
    aero_dex,
    limiter,
):

    print()
    print("=" * 70)
    print("AERODROME FEED")
    print("=" * 70)
    print(
        f"Capital: ${CAPITAL_USDC}"
    )
    print(
        "Ruta: USDC → WETH → cbBTC"
    )
    print()

    while True:

        try:

            # ------------------------------------------------
            # PASO 1
            # USDC → WETH
            # ------------------------------------------------

            quote_1 = await aero_dex.quote(
                USDC_ADDRESS,
                WETH_ADDRESS,
                CAPITAL_USDC,
                stable=False,
            )

            if quote_1 is None:

                print(
                    "[AERO] "
                    "No se pudo cotizar USDC → WETH"
                )

                await asyncio.sleep(
                    AERO_INTERVAL
                )

                continue

            weth_amount = (
                quote_1.amount_out
            )

            # ------------------------------------------------
            # PASO 2
            # WETH → cbBTC
            # ------------------------------------------------

            quote_2 = await aero_dex.quote(
                WETH_ADDRESS,
                CBBTC_ADDRESS,
                weth_amount,
                stable=False,
            )

            if quote_2 is None:

                print(
                    "[AERO] "
                    "No se pudo cotizar WETH → cbBTC"
                )

                await asyncio.sleep(
                    AERO_INTERVAL
                )

                continue

            cbbtc_amount = (
                quote_2.amount_out
            )

            if cbbtc_amount <= 0:

                await asyncio.sleep(
                    AERO_INTERVAL
                )

                continue

            # ------------------------------------------------
            # PRECIO IMPLÍCITO
            # ------------------------------------------------

            implied_price = (
                CAPITAL_USDC
                / cbbtc_amount
            )

            ts = time.time()

            aero.amount_cbBTC = (
                cbbtc_amount
            )

            aero.price = (
                implied_price
            )

            aero.timestamp = ts

            print(
                f"[AERO] "
                f"{now_str()} "
                f"cbBTC=${implied_price:,.2f} "
                f"Output={cbbtc_amount:.8f}"
            )

        except Exception as exc:

            print(
                f"[AERO] "
                f"{type(exc).__name__}: "
                f"{exc}"
            )

        await asyncio.sleep(
            AERO_INTERVAL
        )


# ============================================================
# ARBITRAGE DETECTOR
# ============================================================

async def opportunity_engine():

    print()
    print("=" * 70)
    print("OPPORTUNITY ENGINE")
    print("=" * 70)
    print()

    last_report = 0

    while True:

        await asyncio.sleep(
            0.05
        )

        if (
            binance.bid <= 0
            or binance.ask <= 0
            or aero.price <= 0
        ):
            continue

        # ====================================================
        # COMPARACIÓN
        # ====================================================

        # Binance BID:
        # precio al que podemos vender BTC
        #
        # Binance ASK:
        # precio al que podemos comprar BTC
        #
        # Aerodrome:
        # precio implícito de compra de cbBTC

        aero_buy_price = (
            aero.price
        )

        binance_bid = (
            binance.bid
        )

        binance_ask = (
            binance.ask
        )

        # ====================================================
        # SPREADS
        # ====================================================

        # DEX más caro que Binance

        dex_premium = spread_pct(
            binance_bid,
            aero_buy_price,
        )

        # Binance más caro que DEX

        dex_discount = spread_pct(
            aero_buy_price,
            binance_ask,
        )

        now = time.time()

        # ====================================================
        # REPORTE NORMAL
        # ====================================================

        if now - last_report >= 2:

            last_report = now

            print()
            print(
                f"[MARKET {now_str()}]"
            )

            print(
                f"BINANCE "
                f"Bid=${binance_bid:,.2f} "
                f"Ask=${binance_ask:,.2f}"
            )

            print(
                f"AERODROME "
                f"cbBTC=${aero_buy_price:,.2f}"
            )

            print(
                f"DEX PREMIUM: "
                f"{dex_premium:+.5f}%"
            )

            print(
                f"DEX DISCOUNT: "
                f"{dex_discount:+.5f}%"
            )

        # ====================================================
        # OPORTUNIDAD
        # ====================================================

        if (
            dex_premium
            >= MIN_SPREAD_PCT
        ):

            print()
            print(
                "🔥🔥🔥 POSIBLE DISLOCACIÓN"
            )

            print(
                f"Tiempo: "
                f"{now_str()}"
            )

            print(
                f"Binance BID: "
                f"${binance_bid:,.2f}"
            )

            print(
                f"Aerodrome: "
                f"${aero_buy_price:,.2f}"
            )

            print(
                f"Premium DEX: "
                f"{dex_premium:+.5f}%"
            )

            print(
                f"Capital: "
                f"${CAPITAL_USDC}"
            )

            print(
                "⚠️ BTC vs cbBTC: "
                "NO EJECUTAR todavía"
            )

            print()

        if (
            dex_discount
            >= MIN_SPREAD_PCT
        ):

            print()
            print(
                "🟢🟢🟢 POSIBLE DISLOCACIÓN"
            )

            print(
                f"Tiempo: "
                f"{now_str()}"
            )

            print(
                f"Aerodrome: "
                f"${aero_buy_price:,.2f}"
            )

            print(
                f"Binance ASK: "
                f"${binance_ask:,.2f}"
            )

            print(
                f"Descuento DEX: "
                f"{dex_discount:+.5f}%"
            )

            print(
                "⚠️ BTC vs cbBTC: "
                "NO EJECUTAR todavía"
            )

            print()


# ============================================================
# MAIN
# ============================================================

async def main():

    print()
    print("=" * 80)
    print("              V6 CEX ↔ DEX PRICE ENGINE")
    print("=" * 80)
    print()
    print(
        "Binance BTC/USDT"
    )
    print(
        "        ↕"
    )
    print(
        "Aerodrome cbBTC/USDC"
    )
    print()
    print(
        "MODO: DETECCIÓN / PAPER"
    )
    print(
        "NO EJECUTA TRADES"
    )
    print()
    print("=" * 80)

    # ========================================================
    # WEB3
    # ========================================================

    w3 = await create_web3()

    limiter = RPCRateLimiter(
        min_interval=0.05,
        max_retries=3,
    )

    aero_dex = Aerodrome(
        w3,
        limiter,
    )

    await aero_dex.initialize()

    print(
        f"[AERO] Factory: "
        f"{aero_dex.default_factory}"
    )

    print()

    # ========================================================
    # TASKS
    # ========================================================

    await asyncio.gather(

        binance_feed(),

        aerodrome_feed(
            aero_dex,
            limiter,
        ),

        opportunity_engine(),

    )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    try:

        asyncio.run(
            main()
        )

    except KeyboardInterrupt:

        print()
        print(
            "V6 detenido."
        )

