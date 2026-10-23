# Use SQLite for initial operational state

The initial Testnet executor will store orders, fills, positions, circuit breakers, reconciliations, and audit state in durable SQLite because it has one writer and does not yet need a database service. SQLite will run as a single executor replica on an exclusive persistent volume with WAL, foreign keys, durable synchronization, tested backups, and versioned migrations; multiple executors, high availability, concurrent remote access, or measured database limits require migration to PostgreSQL.
