# syntax=docker/dockerfile:1.7
FROM ghcr.io/astral-sh/uv:0.9.11-python3.13-bookworm-slim AS builder
WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
RUN uv sync --frozen --no-dev --no-editable

FROM python:3.13-slim-bookworm
ARG VERSION=0.1.0
ARG VCS_REF=unknown
LABEL org.opencontainers.image.title="scalping-bot" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.source="https://github.com/raimangsxr/scalping-bot"
RUN useradd --create-home --uid 10001 scalping
WORKDIR /app
COPY --from=builder --chown=scalping:scalping /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH" PYTHONUNBUFFERED=1
USER scalping
ENTRYPOINT ["scalping-bot"]
CMD ["--help"]
