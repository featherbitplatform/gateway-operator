FROM rust:alpine AS builder

# Build-stage-only toolchain deps; versions track the rust:alpine base, so
# pinning (DL3018) would break on every base image refresh. git fetches the
# featherbit gateway crate (git dependency).
# hadolint ignore=DL3018
RUN apk add --no-cache musl-dev g++ make git ca-certificates

# cargo-auditable embeds the crate dependency list into the binary so SBOM
# tools can inventory the FROM scratch image.
RUN cargo install cargo-auditable --locked

# Keep local builds within memory; CI overrides with --build-arg if needed.
ARG CARGO_BUILD_JOBS=4
ENV CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS}

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
# `dist` = release + fat LTO + one codegen unit (Cargo.toml).
RUN cargo auditable build --profile dist --locked

FROM scratch

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /app/target/dist/featherbit-operator /featherbit-operator

EXPOSE 8080 9443

# Non-root (distroless "nonroot" uid).
USER 65532:65532

ENTRYPOINT ["/featherbit-operator"]
CMD ["run"]
