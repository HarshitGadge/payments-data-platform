FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml ./
COPY src/ ./src/
COPY api/ ./api/

RUN pip install --no-cache-dir . "uvicorn[standard]>=0.30" "trino>=0.329"

# Non-root: the serving tier only ever reads, and nothing in the image needs
# elevated privileges.
RUN useradd --create-home --uid 10001 apiuser
USER apiuser

EXPOSE 8000
CMD ["uvicorn", "api.app:app", "--host", "0.0.0.0", "--port", "8000"]
