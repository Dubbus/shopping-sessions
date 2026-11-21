# Shopping Sessions: one static Rust binary in an otherwise empty image.
# Builds for the machine it is built on (the scorer builds natively on linux/amd64).
FROM rust:1-slim-bookworm AS build
WORKDIR /src
COPY pipeline/ ./
# Fully static (glibc), so the final image needs nothing else. On x86-64 the build
# targets Cascade Lake (AVX-512): the scorers are GCP n2 Xeons, Cascade Lake or newer.
# Such an image does not run under emulation on an Apple-silicon Mac; test it on x86.
RUN set -eu; \
    case "$(uname -m)" in \
      x86_64) cpu="-C target-cpu=cascadelake" ;; \
      *) cpu="" ;; \
    esac; \
    target="$(rustc -vV | sed -n 's/^host: //p')"; \
    RUSTFLAGS="-C target-feature=+crt-static $cpu" cargo build --release --locked --target "$target"; \
    cp "target/$target/release/pipeline" /pipeline

FROM scratch
COPY --from=build /pipeline /pipeline
# Runs with no arguments; reads $DATA_DIR, writes $OUTPUT_DIR; exit 0 means done.
CMD ["/pipeline"]
