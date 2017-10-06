# Concurrency-Control-Distributed-Databases
Multi-Version Timestamp Ordering Protocol
Implementation of a main-memory key-value store with Multi-Version Timestamp Ordering (MVTO) protocol.
MVTO is similar to the MVCC protocol. The main difference is that write operations are not buffered.
Thus, reading an uncommitted value is possible and that could cause cascading aborts.
