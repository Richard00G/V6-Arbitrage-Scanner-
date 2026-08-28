from decimal import Decimal

from config import USDC_ADDRESS, WETH_ADDRESS
from v6.optimizer import SCAN_SIZES


BINANCE_FEE = Decimal("0.001")
GAS_USD = Decimal("0.10")


async def quote_dex(
    aero,
    uni,
    dex_name,
    token_in,
    token_out,
    amount,
):
    if dex_name == "AERO":
        return await aero.quote(
            token_in,
            token_out,
            amount,
            stable=False,
        )

    if dex_name == "UNI":
        return await uni.quote(
            token_in,
            token_out,
            amount,
        )

    return None


async def scan_dex_to_cex(
    aero,
    uni,
    dex_name,
    binance_bid,
):
    """
    USDC → DEX → WETH → Binance → USDT

    Escanea distintos tamaños y devuelve
    el resultado económico real de cada quote.
    """

    results = []

    for capital in SCAN_SIZES:

        buy = await quote_dex(
            aero,
            uni,
            dex_name,
            USDC_ADDRESS,
            WETH_ADDRESS,
            capital,
        )

        if buy is None:
            results.append(None)
            continue

        weth = buy.amount_out

        if weth <= 0:
            results.append(None)
            continue

        # Venta en Binance.
        gross_exit = weth * binance_bid

        # Fee Binance.
        cex_fee = (
            gross_exit
            * BINANCE_FEE
        )

        final = (
            gross_exit
            - cex_fee
        )

        gross = (
            final
            - capital
        )

        net = (
            gross
            - GAS_USD
        )

        roi = (
            net
            / capital
        ) * Decimal("100")

        results.append({
            "capital": capital,
            "weth": weth,
            "dex": dex_name,
            "direction": "DEX → CEX",
            "gross": gross,
            "cex_fee": cex_fee,
            "gas": GAS_USD,
            "net": net,
            "roi": roi,
        })

    return results


async def scan_cex_to_dex(
    aero,
    uni,
    dex_name,
    binance_ask,
):
    """
    USDC → Binance → ETH → DEX → USDC

    Escanea distintos tamaños.
    """

    results = []

    for capital in SCAN_SIZES:

        # Binance fee.
        capital_after_fee = (
            capital
            * (
                Decimal("1")
                - BINANCE_FEE
            )
        )

        eth = (
            capital_after_fee
            / binance_ask
        )

        weth = eth

        sell = await quote_dex(
            aero,
            uni,
            dex_name,
            WETH_ADDRESS,
            USDC_ADDRESS,
            weth,
        )

        if sell is None:
            results.append(None)
            continue

        final = sell.amount_out

        gross = (
            final
            - capital
        )

        net = (
            gross
            - GAS_USD
        )

        roi = (
            net
            / capital
        ) * Decimal("100")

        results.append({
            "capital": capital,
            "weth": weth,
            "dex": dex_name,
            "direction": "CEX → DEX",
            "gross": gross,
            "cex_fee": capital * BINANCE_FEE,
            "gas": GAS_USD,
            "net": net,
            "roi": roi,
        })

    return results


def print_size_results(results):
    print()
    print(
        "TAMAÑO → RESULTADO REAL"
    )
    print("-" * 82)

    print(
        f"{'CAPITAL':>12}"
        f"{'WETH':>18}"
        f"{'GROSS':>14}"
        f"{'FEE':>12}"
        f"{'NET':>14}"
        f"{'ROI':>12}"
    )

    print("-" * 82)

    for result in results:

        if result is None:
            continue

        print(
            f"${result['capital']:>10.2f}"
            f"{result['weth']:>18.10f}"
            f"${result['gross']:>13.4f}"
            f"${result['cex_fee']:>11.4f}"
            f"${result['net']:>13.4f}"
            f"{result['roi']:>11.4f}%"
        )

    print("-" * 82)


def best_net(results):

    valid = [
        r
        for r in results
        if r is not None
    ]

    if not valid:
        return None

    return max(
        valid,
        key=lambda r: r["net"],
    )


def best_roi(results):

    valid = [
        r
        for r in results
        if r is not None
    ]

    if not valid:
        return None

    return max(
        valid,
        key=lambda r: r["roi"],
    )


def print_best_execution(result):

    print()

    if result is None:
        print(
            "❌ No existe una ruta válida."
        )
        return

    print(
        "🏆 MEJOR TAMAÑO OBSERVADO"
    )

    print(
        f"Ruta      : {result['direction']}"
    )

    print(
        f"DEX       : {result['dex']}"
    )

    print(
        f"Capital   : "
        f"${result['capital']:.2f}"
    )

    print(
        f"WETH      : "
        f"{result['weth']:.10f}"
    )

    print(
        f"Gross     : "
        f"${result['gross']:+.6f}"
    )

    print(
        f"CEX fee   : "
        f"${result['cex_fee']:.6f}"
    )

    print(
        f"Gas       : "
        f"${result['gas']:.6f}"
    )

    print(
        f"NET       : "
        f"${result['net']:+.6f}"
    )

    print(
        f"ROI       : "
        f"{result['roi']:+.6f}%"
    )

    if result["net"] > 0:
        print(
            "🟢 CANDIDATO RENTABLE"
        )
    else:
        print(
            "🔴 NO RENTABLE"
        )
