FROM python:3.13-slim

ENV UV_LINK_MODE=copy PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY core core
COPY decision decision
COPY sim sim
COPY llm llm
COPY dashboard dashboard
COPY config config
COPY .streamlit .streamlit
RUN uv sync --frozen --no-dev
