import asyncio
from decimal import Decimal

from rpc import create_web3, RPCRateLimiter
from dex.aerodrome import Aerodrome

from config import (
    CBBTC_ADDRESS,
    WETH_ADDRESS,
    USDC_ADDRESS,
)


class AerodromeFeed:

    def __init__(
        self,
        aero,
        amount_usdc=Decimal("100"),
    ):
        self.aero = aero
        self.amount_usdc = amount_usdc

        self.cbbtc = CBBTC_ADDRESS
        self.weth = WETH_ADDRESS
        self.usdc = USDC_ADDRESS

        self.price = None
        self.cbbtc_amount = None
        self.timestamp = None

    async def quote_price(self):

        # ------------------------------------------------
        # USDC → WETH
        # ------------------------------------------------

        usdc_to_weth = await self.aero.quote(
            self.usdc,
            self.weth,
            self.amount_usdc,
            stable=False,
        )

        if usdc_to_weth is None:
            return None

        weth_amount = (
            usdc_to_weth.amount_out
        )

        # ------------------------------------------------
        # WETH → cbBTC
        # ------------------------------------------------

        weth_to_cbbtc = await self.aero.quote(
            self.weth,
            self.cbbtc,
            weth_amount,
            stable=False,
        )

        if weth_to_cbbtc is None:
            return None

        cbbtc_amount = (
            weth_to_cbbtc.amount_out
        )

        if cbbtc_amount <= 0:
            return None

        # ------------------------------------------------
        # PRECIO EFECTIVO
        # ------------------------------------------------

        price = (
            self.amount_usdc
            / cbbtc_amount
        )

        self.price = price
        self.cbbtc_amount = cbbtc_amount

        return price


async def main():

    print("=" * 70)
    print("AERODROME CBBTC PRICE FEED")
    print("=" * 70)

    w3 = await create_web3()

    limiter = RPCRateLimiter(
        min_interval=0.05,
        max_retries=3,
    )

    aero = Aerodrome(
        w3,
        limiter,
    )

    await aero.initialize()

    feed = AerodromeFeed(
        aero,
        Decimal("100"),
    )

    print()
    print("Capital de referencia : $100 USDC")
    print("Ruta                  : USDC → WETH → cbBTC")
    print()

    while True:

        try:

            price = await feed.quote_price()

            if price is None:

                print(
                    "[AERO] ❌ "
                    "No se pudo obtener precio"
                )

            else:

                print(
                    f"[AERO] "
                    f"cbBTC = ${price:.2f}"
                    f" | "
                    f"Output = "
                    f"{feed.cbbtc_amount:.8f}"
                )

        except Exception as exc:

            print(
                f"[AERO] ERROR "
                f"{type(exc).__name__}: {exc}"
            )

        await asyncio.sleep(2)


if __name__ == "__main__":
    asyncio.run(main())
