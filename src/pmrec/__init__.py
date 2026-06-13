"""pmrec - Polymarket research data recorder.

Continuous WebSocket capture of public Polymarket market data (order book,
price changes, trades) plus periodic universe selection and S3 archival.

Read-only, public data only: the Gamma API, the CLOB public data endpoints,
and the public market WebSocket channel. No wallet, no auth, no trading.
"""

__version__ = "0.1.0"
