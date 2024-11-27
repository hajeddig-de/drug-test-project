FROM python:3.9-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk-headless \
    curl \
    procps \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Install Poetry
RUN curl -sSL https://install.python-poetry.org | python3 - \
    && ln -s /root/.local/bin/poetry /usr/local/bin/poetry

# Set working directory
WORKDIR /app

# Copy project files
COPY . .

# Install dependencies
RUN poetry config virtualenvs.in-project true \
    && poetry install --no-dev

# Expose application port
EXPOSE 8000

# Entry point
ENTRYPOINT ["bash", "-c", "export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 && export PATH=$JAVA_HOME/bin:$PATH && poetry run python -m src.__main__"]
