# Use a Python modular monolith with separate runtime processes

The system will use one Python package and one shared domain model for research, market data, strategies, risk, and Testnet execution, while exposing separate processes for data collection, research jobs, and execution. This keeps signal, sizing, stop, and cost semantics identical across backtests and Testnet without coupling operational failure domains or introducing distributed-service complexity before it is justified.
