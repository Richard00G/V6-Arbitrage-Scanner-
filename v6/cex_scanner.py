import asyncio
import json
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal

import websockets

from rpc import create_web3, RPCRateLimiter
from dex.aerodrome import Aerodrome
from dex.uniswap_v3 import UniswapV3
from v6.size_scanner import scan_dex_to_cex, scan_cex_to_dex, best_net, best_roi, print_size_results

from v6.optimizer import SCAN_SIZES, find_best, print_table, print_best
from config import (
    USDC_ADDRESS,
    WETH_ADDRESS,
)

# ============================================================
# V6.7 CEX <-> DEX DISLOCATION EVENT RECORDER
# ============================================================
#
# OBJETIVO:
#
#   NO EJECUTA OPERACIONES.
#
#   Queremos descubrir si las dislocaciones de Aerodrome
#   realmente convergen hacia Uniswap/Binance.
#
#   Registramos:
#
#       - precio Binance
#       - precio Uniswap
#       - precio Aerodrome
#       - premium AERO vs UNI
#       - duración del evento
#       - máximo premium
#       - convergencia
#       - tiempo de convergencia
#
# ============================================================


# ============================================================
# CONFIG
# ============================================================

BINANCE_WS = (
    "wss://stream.binance.com:9443/stream"
)

SYMBOLS = [
    "ethusdt",
    "usdcusdt",
]

REPORT_INTERVAL = 3.0

REFERENCE_CAPITAL = Decimal("100")

# Comienza un evento cuando la diferencia absoluta
# AERO vs UNI supera este porcentaje.
DISLOCATION_THRESHOLD = Decimal("0.20")

# Consideramos que el evento terminó cuando vuelve
# por debajo de este nivel.
CONVERGENCE_THRESHOLD = Decimal("0.05")

# Si no converge dentro de este tiempo, cerramos
# el evento como timeout.
MAX_EVENT_SECONDS = 120.0

# Edad máxima permitida de Binance.
MAX_BINANCE_AGE = 5.0

# Base de datos.
DB_PATH = "data/v6_dislocations.db"


# ============================================================
# MARKET
# ============================================================

@dataclass
class Market:
    symbol: str
    bid: Decimal = Decimal("0")
    ask: Decimal = Decimal("0")
    timestamp: float = 0.0


markets = {
    symbol: Market(symbol)
    for symbol in SYMBOLS
}


# ============================================================
# EVENT STATE
# ============================================================

@dataclass
class Event:
    event_id: int
    started_at: float

    initial_aero: Decimal
    initial_uni: Decimal
    initial_binance: Decimal
    initial_premium: Decimal

    max_premium: Decimal
    min_premium: Decimal

    last_premium: Decimal
    last_aero: Decimal
    last_uni: Decimal

    observations: int = 1


active_event = None
event_counter = 0


# ============================================================
# TIME
# ============================================================

def now():
    return datetime.now(
        timezone.utc
    ).strftime(
        "%H:%M:%S.%f"
    )[:-3]


def iso_now():
    return datetime.now(
        timezone.utc
    ).isoformat()


# ============================================================
# DATABASE
# ============================================================

def init_db():
    conn = sqlite3.connect(DB_PATH)

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,

            started_at TEXT NOT NULL,
            ended_at TEXT,

            duration_seconds REAL,

            initial_aero REAL,
            initial_uni REAL,
            initial_binance REAL,

            initial_premium REAL,

            max_premium REAL,
            min_premium REAL,

            final_aero REAL,
            final_uni REAL,

            final_premium REAL,

            observations INTEGER,

            converged INTEGER,

            convergence_seconds REAL,

            reason TEXT
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS observations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,

            timestamp TEXT NOT NULL,

            binance_price REAL,
            uniswap_price REAL,
            aerodrome_price REAL,

            premium_aero_uni REAL,
            premium_aero_binance REAL,

            event_id INTEGER
        )
        """
    )

    conn.commit()
    conn.close()


def save_observation(
    binance_price,
    uni_price,
    aero_price,
    premium_aero_uni,
    premium_aero_binance,
    event_id,
):
    conn = sqlite3.connect(DB_PATH)

    conn.execute(
        """
        INSERT INTO observations (
            timestamp,
            binance_price,
            uniswap_price,
            aerodrome_price,
            premium_aero_uni,
            premium_aero_binance,
            event_id
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            iso_now(),
            float(binance_price),
            float(uni_price),
            float(aero_price),
            float(premium_aero_uni),
            float(premium_aero_binance),
            event_id,
        ),
    )

    conn.commit()
    conn.close()


def save_event(
    event,
    ended_at,
    final_aero,
    final_uni,
    final_premium,
    converged,
    convergence_seconds,
    reason,
):
    conn = sqlite3.connect(DB_PATH)

    duration = (
        ended_at
        - event.started_at
    )

    conn.execute(
        """
        INSERT INTO events (
            started_at,
            ended_at,
            duration_seconds,

            initial_aero,
            initial_uni,
            initial_binance,

            initial_premium,

            max_premium,
            min_premium,

            final_aero,
            final_uni,

            final_premium,

            observations,

            converged,
            convergence_seconds,

            reason
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            datetime.fromtimestamp(
                event.started_at,
                timezone.utc,
            ).isoformat(),

            datetime.fromtimestamp(
                ended_at,
                timezone.utc,
            ).isoformat(),

            duration,

            float(event.initial_aero),
            float(event.initial_uni),
            float(event.initial_binance),

            float(event.initial_premium),

            float(event.max_premium),
            float(event.min_premium),

            float(final_aero),
            float(final_uni),

            float(final_premium),

            event.observations,

            1 if converged else 0,

            convergence_seconds,

            reason,
        ),
    )

    conn.commit()
    conn.close()


# ============================================================
# BINANCE FEED
# ============================================================

async def binance_feed():

    streams = "/".join(
        f"{symbol}@bookTicker"
        for symbol in SYMBOLS
    )

    url = (
        f"{BINANCE_WS}?streams={streams}"
    )

    print("=" * 80)
    print("BINANCE MARKET FEED")
    print("=" * 80)

    for symbol in SYMBOLS:
        print(
            f"  • {symbol.upper()}"
        )

    print()

    while True:

        try:

            async with websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=20,
            ) as ws:

                print(
                    "[BINANCE] WebSocket conectado"
                )

                async for message in ws:

                    payload = json.loads(
                        message
                    )

                    data = payload.get(
                        "data"
                    )

                    if not data:
                        continue

                    symbol = (
                        data["s"]
                        .lower()
                    )

                    if symbol not in markets:
                        continue

                    markets[
                        symbol
                    ].bid = Decimal(
                        data["b"]
                    )

                    markets[
                        symbol
                    ].ask = Decimal(
                        data["a"]
                    )

                    markets[
                        symbol
                    ].timestamp = (
                        time.time()
                    )

        except Exception as exc:

            print(
                f"[BINANCE] ERROR "
                f"{type(exc).__name__}: "
                f"{exc}"
            )

            print(
                "[BINANCE] "
                "Reconectando en 2 segundos..."
            )

            await asyncio.sleep(2)


# ============================================================
# BINANCE ETH/USDC
# ============================================================

def binance_eth_usdc():

    eth = markets["ethusdt"]
    usdc = markets["usdcusdt"]

    if (
        eth.bid <= 0
        or eth.ask <= 0
        or usdc.bid <= 0
        or usdc.ask <= 0
    ):
        return None

    age_eth = (
        time.time()
        - eth.timestamp
    )

    age_usdc = (
        time.time()
        - usdc.timestamp
    )

    if (
        age_eth > MAX_BINANCE_AGE
        or age_usdc > MAX_BINANCE_AGE
    ):
        return None

    bid = (
        eth.bid
        / usdc.ask
    )

    ask = (
        eth.ask
        / usdc.bid
    )

    mid = (
        bid + ask
    ) / Decimal("2")

    return {
        "bid": bid,
        "ask": ask,
        "mid": mid,
    }


# ============================================================
# DEX QUOTE
# ============================================================

async def dex_quote(
    aero,
    uni,
    dex,
    token_in,
    token_out,
    amount,
):

    if dex == "AERO":

        return await aero.quote(
            token_in,
            token_out,
            amount,
            stable=False,
        )

    if dex == "UNI":

        return await uni.quote(
            token_in,
            token_out,
            amount,
        )

    return None


# ============================================================
# DEX EFFECTIVE BUY PRICE
# ============================================================

async def dex_buy_price(
    aero,
    uni,
    dex,
    capital,
):

    quote = await dex_quote(
        aero,
        uni,
        dex,
        USDC_ADDRESS,
        WETH_ADDRESS,
        capital,
    )

    if quote is None:
        return None

    if quote.amount_out <= 0:
        return None

    return (
        capital
        / quote.amount_out
    )


# ============================================================
# MAIN MARKET SAMPLE
# ============================================================

async def sample_market(
    aero,
    uni,
):

    cex = binance_eth_usdc()

    if cex is None:
        print(
            f"[{now()}] "
            "Esperando Binance..."
        )

        return None

    # --------------------------------------------------------
    # AERODROME
    # --------------------------------------------------------

    aero_buy = await dex_buy_price(
        aero,
        uni,
        "AERO",
        REFERENCE_CAPITAL,
    )

    if aero_buy is None:
        print(
            f"[{now()}] "
            "Aerodrome sin quote."
        )

        return None

    # --------------------------------------------------------
    # UNISWAP
    # --------------------------------------------------------

    uni_buy = await dex_buy_price(
        aero,
        uni,
        "UNI",
        REFERENCE_CAPITAL,
    )

    if uni_buy is None:
        print(
            f"[{now()}] "
            "Uniswap sin quote."
        )

        return None

    # --------------------------------------------------------
    # PREMIUM
    # --------------------------------------------------------

    premium_uni = (
        (
            aero_buy
            - uni_buy
        )
        / uni_buy
    ) * Decimal("100")

    premium_binance = (
        (
            aero_buy
            - cex["mid"]
        )
        / cex["mid"]
    ) * Decimal("100")

    return {
        "binance": cex["mid"],
        "binance_bid": cex["bid"],
        "binance_ask": cex["ask"],
        "aero": aero_buy,
        "uni": uni_buy,
        "premium_uni": premium_uni,
        "premium_binance": premium_binance,
    }


# ============================================================
# EVENT START
# ============================================================

def start_event(data):

    global active_event
    global event_counter

    event_counter += 1

    premium = data["premium_uni"]

    active_event = Event(
        event_id=event_counter,

        started_at=time.time(),

        initial_aero=data["aero"],
        initial_uni=data["uni"],
        initial_binance=data["binance"],

        initial_premium=premium,

        max_premium=premium,
        min_premium=premium,

        last_premium=premium,

        last_aero=data["aero"],
        last_uni=data["uni"],
    )

    print()
    print("=" * 80)
    print(
        f"🔥 EVENTO #{event_counter} INICIADO"
    )
    print("=" * 80)

    print(
        f"AERO       : "
        f"${data['aero']:,.4f}"
    )

    print(
        f"UNI        : "
        f"${data['uni']:,.4f}"
    )

    print(
        f"BINANCE    : "
        f"${data['binance']:,.4f}"
    )

    print(
        f"PREMIUM    : "
        f"{premium:+.4f}%"
    )

    print("=" * 80)


# ============================================================
# EVENT UPDATE
# ============================================================

def update_event(data):

    global active_event

    if active_event is None:
        return

    premium = data["premium_uni"]

    active_event.last_premium = premium

    active_event.last_aero = data["aero"]
    active_event.last_uni = data["uni"]

    active_event.observations += 1

    if premium > active_event.max_premium:

        active_event.max_premium = premium

    if premium < active_event.min_premium:

        active_event.min_premium = premium


# ============================================================
# EVENT END
# ============================================================

def end_event(
    data,
    converged,
    reason,
):

    global active_event

    if active_event is None:
        return

    ended_at = time.time()

    duration = (
        ended_at
        - active_event.started_at
    )

    convergence_seconds = (
        duration
        if converged
        else None
    )

    save_event(
        active_event,
        ended_at,

        data["aero"],
        data["uni"],
        data["premium_uni"],

        converged,

        convergence_seconds,

        reason,
    )

    print()
    print("=" * 80)

    if converged:

        print(
            f"✅ EVENTO #{active_event.event_id} "
            "CONVERGIÓ"
        )

    else:

        print(
            f"⏱️ EVENTO #{active_event.event_id} "
            "CERRADO POR TIMEOUT"
        )

    print(
        f"Duración      : "
        f"{duration:.2f}s"
    )

    print(
        f"Observaciones : "
        f"{active_event.observations}"
    )

    print(
        f"Inicial       : "
        f"{active_event.initial_premium:+.4f}%"
    )

    print(
        f"Máximo        : "
        f"{active_event.max_premium:+.4f}%"
    )

    print(
        f"Mínimo        : "
        f"{active_event.min_premium:+.4f}%"
    )

    print(
        f"Final         : "
        f"{data['premium_uni']:+.4f}%"
    )

    print(
        f"Motivo        : "
        f"{reason}"
    )

    print("=" * 80)

    active_event = None


# ============================================================
# V6.8 SIZE OPTIMIZER HOOK
# ============================================================

async def run_v68_size_scan(aero, uni, data):
    """
    Escanea tamaños reales contra los quotes actuales.

    NO EJECUTA OPERACIONES.
    Solo calcula:
        - gross
        - fees
        - gas
        - net
        - ROI

    Busca tanto:
        DEX -> CEX
        CEX -> DEX
    """

    print()
    print("=" * 80)
    print("V6.8 SIZE SCAN")
    print("=" * 80)

    binance_bid = data["binance_bid"]
    binance_ask = data["binance_ask"]

    all_results = []

    for dex_name in ("AERO", "UNI"):

        # ----------------------------------------------------
        # DEX -> CEX
        # ----------------------------------------------------

        results_dex_cex = await scan_dex_to_cex(
            aero,
            uni,
            dex_name,
            binance_bid,
        )

        print()
        print(f"--- {dex_name} DEX -> CEX ---")
        print_size_results(results_dex_cex)

        all_results.extend(
            r for r in results_dex_cex
            if r is not None
        )

        # ----------------------------------------------------
        # CEX -> DEX
        # ----------------------------------------------------

        results_cex_dex = await scan_cex_to_dex(
            aero,
            uni,
            dex_name,
            binance_ask,
        )

        print()
        print(f"--- {dex_name} CEX -> DEX ---")
        print_size_results(results_cex_dex)

        all_results.extend(
            r for r in results_cex_dex
            if r is not None
        )

    if not all_results:
        print()
        print("❌ V6.8: ningún quote válido.")
        print("=" * 80)
        return None

    profitable = [
        r for r in all_results
        if r["net"] > 0
    ]

    print()
    print("=" * 80)

    if not profitable:
        best = max(
            all_results,
            key=lambda r: r["net"],
        )

        print("❌ NINGÚN TAMAÑO ES RENTABLE")
        print(
            f"Mejor candidato : "
            f"{best['direction']} {best['dex']}"
        )
        print(
            f"Capital         : "
            f"${best['capital']:.2f}"
        )
        print(
            f"NET             : "
            f"${best['net']:+.6f}"
        )
        print(
            f"ROI             : "
            f"{best['roi']:+.6f}%"
        )

        print("=" * 80)
        return best

    # --------------------------------------------------------
    # MEJOR NETO
    # --------------------------------------------------------

    best = max(
        profitable,
        key=lambda r: r["net"],
    )

    # --------------------------------------------------------
    # MEJOR ROI
    # --------------------------------------------------------

    best_r = max(
        profitable,
        key=lambda r: r["roi"],
    )

    print("🔥 V6.8 OPORTUNIDAD EJECUTABLE TEÓRICA")
    print()
    print(
        f"MEJOR NETO"
    )
    print(
        f"  Dirección : {best['direction']}"
    )
    print(
        f"  DEX       : {best['dex']}"
    )
    print(
        f"  Capital   : ${best['capital']:.2f}"
    )
    print(
        f"  Gross     : ${best['gross']:+.6f}"
    )
    print(
        f"  Fee CEX   : ${best['cex_fee']:.6f}"
    )
    print(
        f"  Gas       : ${best['gas']:.6f}"
    )
    print(
        f"  NET       : ${best['net']:+.6f}"
    )
    print(
        f"  ROI       : {best['roi']:+.6f}%"
    )

    print()
    print("MEJOR ROI")
    print(
        f"  Dirección : {best_r['direction']}"
    )
    print(
        f"  DEX       : {best_r['dex']}"
    )
    print(
        f"  Capital   : ${best_r['capital']:.2f}"
    )
    print(
        f"  NET       : ${best_r['net']:+.6f}"
    )
    print(
        f"  ROI       : {best_r['roi']:+.6f}%"
    )

    print("=" * 80)

    return best

# ============================================================
# EVENT ENGINE
# ============================================================

async def process_sample(
    aero,
    uni,
):

    data = await sample_market(
        aero,
        uni,
    )

    if data is None:
        return

    premium = data["premium_uni"]

    # --------------------------------------------------------
    # GUARDAR OBSERVACIÓN
    # --------------------------------------------------------

    event_id = (
        active_event.event_id
        if active_event is not None
        else None
    )

    save_observation(
        data["binance"],
        data["uni"],
        data["aero"],
        data["premium_uni"],
        data["premium_binance"],
        event_id,
    )

    # --------------------------------------------------------
    # DISPLAY
    # --------------------------------------------------------

    print()
    print("=" * 80)

    print(
        f"[V6.7 {now()}]"
    )

    print("-" * 80)

    print(
        "BINANCE ETH/USDC"
    )

    print(
        f"  MID : "
        f"${data['binance']:,.4f}"
    )

    print(
        "UNISWAP"
    )

    print(
        f"  BUY : "
        f"${data['uni']:,.4f}"
    )

    print(
        "AERODROME"
    )

    print(
        f"  BUY : "
        f"${data['aero']:,.4f}"
    )

    print("-" * 80)

    print(
        f"AERO vs UNI       : "
        f"{premium:+.4f}%"
    )

    print(
        f"AERO vs BINANCE   : "
        f"{data['premium_binance']:+.4f}%"
    )

    # --------------------------------------------------------
    # EVENT MANAGEMENT
    # --------------------------------------------------------

    if active_event is None:

        if (
            abs(premium)
            >= DISLOCATION_THRESHOLD
        ):

            start_event(data)
            
            await run_v68_size_scan(
                aero,
                uni,
                data,
            )

        return

    # --------------------------------------------------------
    # UPDATE
    # --------------------------------------------------------

    update_event(data)

    duration = (
        time.time()
        - active_event.started_at
    )

    # --------------------------------------------------------
    # CONVERGENCE
    # --------------------------------------------------------

    if (
        abs(premium)
        <= CONVERGENCE_THRESHOLD
    ):

        end_event(
            data,
            True,
            "CONVERGENCIA",
        )

        return

    # --------------------------------------------------------
    # TIMEOUT
    # --------------------------------------------------------

    if (
        duration
        >= MAX_EVENT_SECONDS
    ):

        end_event(
            data,
            False,
            "TIMEOUT",
        )

        return

    # --------------------------------------------------------
    # LIVE EVENT
    # --------------------------------------------------------

    print(
        f"EVENTO #{active_event.event_id} "
        f"| duración={duration:.1f}s "
        f"| máximo={active_event.max_premium:+.4f}%"
    )


# ============================================================
# STATS
# ============================================================

def print_stats():

    conn = sqlite3.connect(DB_PATH)

    row = conn.execute(
        """
        SELECT
            COUNT(*),
            COALESCE(
                SUM(converged),
                0
            ),
            AVG(duration_seconds),
            AVG(
                CASE
                    WHEN converged = 1
                    THEN convergence_seconds
                END
            ),
            MAX(max_premium),
            MIN(min_premium)
        FROM events
        """
    ).fetchone()

    conn.close()

    total = row[0] or 0
    converged = row[1] or 0
    avg_duration = row[2]
    avg_convergence = row[3]
    max_premium = row[4]
    min_premium = row[5]

    print()
    print("=" * 80)
    print("V6.7 ESTADÍSTICAS ACUMULADAS")
    print("=" * 80)

    print(
        f"Eventos totales     : {total}"
    )

    print(
        f"Convergencias       : {converged}"
    )

    if total > 0:

        rate = (
            Decimal(converged)
            / Decimal(total)
            * Decimal("100")
        )

        print(
            f"Tasa convergencia   : "
            f"{rate:.2f}%"
        )

    if avg_duration is not None:

        print(
            f"Duración media      : "
            f"{avg_duration:.2f}s"
        )

    if avg_convergence is not None:

        print(
            f"Convergencia media  : "
            f"{avg_convergence:.2f}s"
        )

    if max_premium is not None:

        print(
            f"Máximo observado   : "
            f"{max_premium:+.4f}%"
        )

    if min_premium is not None:

        print(
            f"Mínimo observado   : "
            f"{min_premium:+.4f}%"
        )

    print("=" * 80)


# ============================================================
# ENGINE
# ============================================================

async def engine(
    aero,
    uni,
):

    last_stats = time.time()

    while True:

        try:

            await process_sample(
                aero,
                uni,
            )

        except Exception as exc:

            print(
                f"[ENGINE] ERROR "
                f"{type(exc).__name__}: "
                f"{exc}"
            )

        # ----------------------------------------------------
        # ESTADÍSTICAS CADA 60 SEGUNDOS
        # ----------------------------------------------------

        if (
            time.time()
            - last_stats
            >= 60
        ):

            print_stats()

            last_stats = time.time()

        await asyncio.sleep(
            REPORT_INTERVAL
        )


# ============================================================
# MAIN
# ============================================================

async def main():

    print()
    print("=" * 80)
    print(
        "       V6.7 CEX <-> DEX EVENT RECORDER"
    )
    print("=" * 80)

    print(
        "MODO: SOLO LECTURA"
    )

    print(
        "NO EJECUTA OPERACIONES"
    )

    print()

    print(
        f"Capital referencia : "
        f"${REFERENCE_CAPITAL}"
    )

    print(
        f"Dislocation       : "
        f"{DISLOCATION_THRESHOLD}%"
    )

    print(
        f"Convergencia      : "
        f"{CONVERGENCE_THRESHOLD}%"
    )

    print(
        f"Timeout evento    : "
        f"{MAX_EVENT_SECONDS}s"
    )

    print(
        f"Database           : "
        f"{DB_PATH}"
    )

    print()

    init_db()

    w3 = await create_web3()

    limiter = RPCRateLimiter(
        min_interval=0.05,
        max_retries=3,
    )

    aero = Aerodrome(
        w3,
        limiter,
    )

    uni = UniswapV3(
        w3,
        limiter,
    )

    await aero.initialize()

    print(
        f"[AERO] Factory: "
        f"{aero.default_factory}"
    )

    print()

    await asyncio.gather(
        binance_feed(),
        engine(
            aero,
            uni,
        ),
    )


# ============================================================
# ENTRY
# ============================================================

if __name__ == "__main__":

    try:

        asyncio.run(
            main()
        )

    except KeyboardInterrupt:

        print()
        print(
            "V6.7 detenido."
        )

        print_stats()
