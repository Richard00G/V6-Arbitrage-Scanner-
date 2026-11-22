# V6 Arbitrage Scanner (CEX ↔ DEX en Base)

Herramienta de investigación **de solo lectura** para medir dislocaciones de precio entre Binance y los DEX de Base (Aerodrome y Uniswap V3). **No ejecuta operaciones.**

> Estado: **archivado**. Este proyecto cumplió su objetivo de investigación y fue el punto de partida de **NEXA Engine** (detección de liquidaciones).

## Hipótesis

Los precios de ETH y BTC en los DEX de Base (Aerodrome / Uniswap V3) se separan por momentos del precio de Binance, y esas separaciones ("dislocaciones") podrían ser lo bastante grandes y duraderas como para un arbitraje CEX ↔ DEX rentable.

## Qué hace

| Módulo | Función |
|---|---|
| `v6/price_engine.py` | Compara Binance BTC/USDT contra el precio implícito de cbBTC en Aerodrome (ruta USDC → WETH → cbBTC). |
| `v6/cex_scanner.py` | **Registrador de eventos (V6.7 / V6.8).** Compara Binance ETH/USDC, Uniswap V3 y Aerodrome. Cuando el premium AERO vs UNI pasa del umbral, abre un evento y registra duración, premium máximo/mínimo y si convergió o se cerró por timeout. Guarda todo en SQLite (`data/v6_dislocations.db`). |
| `v6/size_scanner.py` | Para cada evento, simula distintos tamaños de capital ($25 → $10,000) en ambas direcciones (DEX → CEX y CEX → DEX) con comisión de Binance y gas estimado. |
| `v6/optimizer.py` | Utilidades para escoger el tamaño con mejor neto / mejor ROI. |
| `v6/cex/binance.py` | Feed de Binance por WebSocket (`bookTicker`) con reconexión. |
| `v6/dex/aerodrome_feed.py` | Feed de precio de cbBTC en Aerodrome. |

## Hallazgos

1. **La mayoría de las "dislocaciones" no eran oportunidades reales.** Con umbrales de 0.10–0.20 %, buena parte del premium se explicaba por las comisiones de los pools, el impacto de precio y comparar precios de momentos distintos.
2. **El arbitraje CEX ↔ DEX no es atómico.** Requiere inventario en los dos lados o mover fondos entre Base y Binance (minutos y comisiones). El modelo de costos solo incluía la comisión de Binance y un gas fijo.
3. **El mercado está dominado por bots profesionales.** En Base, este arbitraje lo capturan bots con infraestructura de baja latencia, inventario en los dos lados y pujas de prioridad dentro del bloque. Consultar cada 3 s no compite.
4. **El aprendizaje útil:** al estudiar *por qué* y *cuándo* se movían los precios, aparecieron las **liquidaciones** como una fuente de movimiento más clara y explotable. De ahí nació NEXA Engine.

## Limitaciones conocidas

- Solo se cotizan pools volátiles clásicos de Aerodrome (`stable=False`), no Slipstream (liquidez concentrada), donde está la mayor parte de la liquidez de WETH/USDC.
- Las cotizaciones on-chain son secuenciales y no se fijan a un mismo bloque.
- El gas está fijo (`GAS_USD = 0.10`) y se asume USDC ≈ USDT en el cálculo DEX → CEX.
- Sin costos de depósito/retiro entre la cadena y Binance.

## Cómo correrlo

El repo **no incluye** los módulos compartidos del proyecto original. Para correrlo hay que agregar en la raíz:

- `config.py` → `USDC_ADDRESS`, `WETH_ADDRESS`, `CBBTC_ADDRESS` (ver `config.example.py`)
- `rpc.py` → `create_web3()` (Web3 asíncrono conectado a Base) y `RPCRateLimiter(min_interval, max_retries)`
- `dex/aerodrome.py` → clase `Aerodrome(w3, limiter)` con `initialize()`, `default_factory` y `quote(token_in, token_out, amount, stable)` que regrese un objeto con `amount_out`
- `dex/uniswap_v3.py` → clase `UniswapV3(w3, limiter)` con `quote(token_in, token_out, amount)`

Luego:

```bash
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
python -m v6.cex_scanner
```

## Componentes reutilizables

- Feed de Binance por WebSocket con reconexión automática.
- Registrador de eventos (inicio → máximo → convergencia o timeout) en SQLite: sirve para estudiar cualquier evento de mercado, como las liquidaciones.
