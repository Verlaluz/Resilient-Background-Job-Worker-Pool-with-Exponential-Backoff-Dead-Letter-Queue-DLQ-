FROM rust:latest
WORKDIR /usr/src/task_worker_pool
COPY . .
RUN cargo build --release
ENTRYPOINT ["./target/release/task_worker_pool"]