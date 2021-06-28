# Rust Concurrent SQS Worker

An experiment built on the Rust Tokio async runtime. Minimises SQS
costs by long polling for the maximum of 10 messages from an SQS 
queue. Messages are distributed to a concurrent worker pool. Can be 
further improved by batching delete message requests.

Produce some messages for consumption:

    RUST_LOG=INFO cargo run --bin producer

Consume the messages:

    RUST_LOG=INFO cargo run --bin consumer

Configure consumer concurrency and producer message volume in `Settings.toml`

Use the terraform files in `/infra` to provision a queue.
