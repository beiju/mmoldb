FROM mcr.microsoft.com/devcontainers/rust:1-1-bookworm AS base

# Include lld linker to improve build times either by using environment variable
# RUSTFLAGS="-C link-arg=-fuse-ld=lld" or with Cargo's configuration file (i.e see .cargo/config.toml).
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
   && apt-get -y install clang lld \
   && apt-get autoremove -y && apt-get clean -y

RUN cargo install cargo-build-dependencies

FROM base AS setup

WORKDIR /code
RUN cargo init
COPY ../.cargo /code/.cargo
COPY ../Cargo.toml ../Cargo.lock /code/
RUN cargo fetch --manifest-path /code/Cargo.toml --locked
# RUN cargo build-dependencies --release  # third-party tool

FROM setup AS builder

# Yes, I do need all these separate copies. No, I'm not happy about it.
COPY ../build.rs /code/
COPY ../migrations /code/migrations
COPY ../src /code/src
COPY ../schema_docs /code/schema_docs
RUN cargo build --release --offline

FROM debian:bookworm

RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
   && apt-get -y install libssl3 libpq5 ca-certificates \
   && apt-get autoremove -y && apt-get clean -y

ENV ROCKET_ADDRESS=0.0.0.0

EXPOSE 42424

COPY --from=builder /code/target/release/mmoldb /mmoldb

WORKDIR /code

# Rocket app depends on some files in the code
# NOTE: You can't combine the folder copies into one or they will be flattened.
# There's no easy way to disable this behavior: https://stackoverflow.com/questions/37715224/copy-multiple-directories-with-one-command
COPY ../static /code/static
COPY ../templates /code/templates
COPY ../Rocket.toml /code

CMD [ "/mmoldb" ]
