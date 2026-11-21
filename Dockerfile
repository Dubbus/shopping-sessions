# syntax=docker/dockerfile:1
# Build has network (10 min cap); run has none. Scored image is linux/amd64, <= 8 GB.
# No platform pinned, so `make build PLATFORM=linux/arm64` gives a fast native image on a Mac.

FROM rust:1.85-slim-bookworm AS build
ARG TARGETARCH
WORKDIR /src

# Scorers are GCP n2 (Cascade Lake or newer, AVX-512). x86-64-v3 = AVX2/BMI2, safe on all
# of them; try CPU=x86-64-v4 for AVX-512 once there's something to measure.
ARG CPU=x86-64-v3
RUN if [ "$TARGETARCH" = "amd64" ]; then echo "-C target-cpu=${CPU}" > /rustflags; \
    else echo "-C target-cpu=native" > /rustflags; fi

# Cache deps separately from source.
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo 'fn main(){}' > src/main.rs \
 && RUSTFLAGS="$(cat /rustflags)" cargo build --release \
 && rm -rf src target/release/deps/sessions* target/release/sessions*

COPY src ./src
RUN RUSTFLAGS="$(cat /rustflags)" cargo build --release

# distroless/cc: glibc + libgcc only. Tiny image, fast container start.
FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=build /src/target/release/sessions /sessions
USER 1000:1000
ENTRYPOINT ["/sessions"]
