# Local environment identical to CI: Python 3.11 + Java + Spark + Delta.
FROM python:3.11-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir -e ".[dev,ml]"

# Download the Delta JARs at build time, so running does not depend on network access.
RUN python -c "from housing_lakehouse.spark import get_spark; get_spark('warmup').stop()"

COPY . .
ENV REQUIRE_SPARK=1
CMD ["pytest"]
