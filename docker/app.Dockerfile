# Image des producteurs : simulateur de bornes et collecteur CO₂ RTE (ADR 001).
FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /app

COPY requirements-app.txt .
RUN pip install -r requirements-app.txt

COPY pyproject.toml ./
COPY src ./src
RUN pip install --no-deps -e .
COPY tests ./tests

RUN useradd --create-home --uid 1000 app && chown -R app:app /app
USER app

CMD ["python", "-m", "lumio_stream.simulator"]
