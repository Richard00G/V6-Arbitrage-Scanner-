from decimal import Decimal

# ============================================================
# V6.8 REAL EXECUTION SIZE OPTIMIZER
# ============================================================

SCAN_SIZES = [
    Decimal("25"),
    Decimal("50"),
    Decimal("100"),
    Decimal("250"),
    Decimal("500"),
    Decimal("1000"),
    Decimal("2000"),
    Decimal("5000"),
    Decimal("10000"),
]


def calculate_result(
    capital,
    weth_received,
    exit_price,
    cex_fee,
    gas,
):
    """
    Calcula el resultado económico de una ruta.

    capital:
        USDC inicial

    weth_received:
        WETH obtenido en el DEX

    exit_price:
        precio al que podemos vender WETH

    cex_fee:
        comisión CEX

    gas:
        coste estimado de gas
    """

    if capital <= 0:
        return None

    if weth_received <= 0:
        return None

    gross = (
        weth_received
        * exit_price
        - capital
    )

    fee = (
        weth_received
        * exit_price
        * cex_fee
    )

    net = (
        gross
        - fee
        - gas
    )

    roi = (
        net
        / capital
    ) * Decimal("100")

    return {
        "capital": capital,
        "weth": weth_received,
        "gross": gross,
        "fee": fee,
        "gas": gas,
        "net": net,
        "roi": roi,
    }


def find_best(results):
    """
    Selecciona el tamaño con mayor beneficio
    neto absoluto.
    """

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


def find_best_roi(results):
    """
    Selecciona el tamaño con mayor ROI.
    """

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


def print_table(results):
    print()
    print(
        "OPTIMIZACIÓN DE TAMAÑO"
    )
    print("-" * 80)

    print(
        f"{'CAPITAL':>12}"
        f"{'GROSS':>14}"
        f"{'FEE':>12}"
        f"{'GAS':>10}"
        f"{'NET':>14}"
        f"{'ROI':>12}"
    )

    print("-" * 80)

    for r in results:

        if r is None:
            continue

        print(
            f"${r['capital']:>10.2f}"
            f"${r['gross']:>13.4f}"
            f"${r['fee']:>11.4f}"
            f"${r['gas']:>9.4f}"
            f"${r['net']:>13.4f}"
            f"{r['roi']:>11.4f}%"
        )

    print("-" * 80)


def print_best(best):
    if best is None:
        print(
            "❌ No existe tamaño válido."
        )
        return

    print()
    print(
        "🏆 CAPITAL ÓPTIMO"
    )
    print(
        f"Capital : ${best['capital']:.2f}"
    )
    print(
        f"Gross   : ${best['gross']:+.6f}"
    )
    print(
        f"Fee     : ${best['fee']:.6f}"
    )
    print(
        f"Gas     : ${best['gas']:.6f}"
    )
    print(
        f"NET     : ${best['net']:+.6f}"
    )
    print(
        f"ROI     : {best['roi']:+.6f}%"
    )

    if best["net"] > 0:
        print(
            "🟢 CANDIDATO ECONÓMICAMENTE RENTABLE"
        )
    else:
        print(
            "🔴 NO RENTABLE DESPUÉS DE COSTES"
        )
