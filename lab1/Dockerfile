FROM ghcr.io/astral-sh/uv:0.5.5-python3.12-bookworm-slim

WORKDIR /app

COPY pyproject.toml .
RUN uv pip install -r pyproject.toml --system

COPY . .

EXPOSE 8501

CMD ["streamlit", "run", "app.py"]
