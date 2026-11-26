# Shopping Sessions: one static Rust binary in an otherwise empty image.
# Builds for the machine it is built on (the scorer builds natively on linux/amd64).
FROM rust:1-slim-bookworm AS build
WORKDIR /src
COPY pipeline/ pipeline/
COPY tools/generator/ generator/
# Fully static (glibc), so the final image needs nothing else. On x86-64 the build
# targets Cascade Lake (AVX-512): the scorers are GCP n2 Xeons, Cascade Lake or newer.
# Such an image does not run under emulation on an Apple-silicon Mac; test it on x86.
#
# The build is profile-guided: an instrumented binary runs once on a 2M-event dataset
# made here with the organisers' generator, and the real build uses what it recorded.
# If any part of that fails, the binary is built without a profile instead.
RUN set -eu; \
    case "$(uname -m)" in \
      x86_64) cpu="-C target-cpu=cascadelake" ;; \
      *) cpu="" ;; \
    esac; \
    target="$(rustc -vV | sed -n 's/^host: //p')"; \
    flags="-C target-feature=+crt-static $cpu"; \
    profile=""; \
    if rustup component add llvm-tools-preview \
       && (cd generator && cargo build --release --locked) \
       && generator/target/release/generate --seed 20261121 --events 2000000 --clicks 500000 --output /train >/dev/null \
       && (cd pipeline && RUSTFLAGS="$flags -Cprofile-generate=/pgo" cargo build --release --locked --target "$target" --target-dir /instrumented) \
       && "/instrumented/$target/release/pipeline" /train /train-out \
       && "$(find "$(rustc --print sysroot)" -name llvm-profdata | head -1)" merge -o /pgo/merged.profdata /pgo; then \
      profile="-Cprofile-use=/pgo/merged.profdata"; \
    else \
      echo "profile-guided build unavailable; building without a profile" >&2; \
    fi; \
    cd pipeline; \
    RUSTFLAGS="$flags $profile" cargo build --release --locked --target "$target"; \
    cp "target/$target/release/pipeline" /pipeline

FROM scratch
COPY --from=build /pipeline /pipeline
# Runs with no arguments; reads $DATA_DIR, writes $OUTPUT_DIR; exit 0 means done.
CMD ["/pipeline"]
