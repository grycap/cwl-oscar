FROM python:3.11-slim

# Set metadata
LABEL maintainer="CWL-OSCAR contributors"
LABEL description="CWL executor for OSCAR clusters"
LABEL version="0.1.0"

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PATH="/app:$PATH"

# Install system dependencies and Node.js
RUN apt-get update && apt-get install -y \
    git \
    curl \
    gnupg \
    ca-certificates \
    && curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y nodejs \
    && npm install -g npm@latest \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY cwl_oscar/ ./cwl_oscar/
COPY cwl-oscar .
COPY README.md .

# Create placeholder for build info file (will be populated by docker-run.sh)
RUN touch ./cwl_oscar/.build_info

# Make the main script executable
RUN chmod +x cwl-oscar

# Create examples directory and copy examples
COPY cwl_oscar/example/ ./examples/
COPY cwl-example/ ./cwl-example/

# Create a non-root user for security
RUN adduser --disabled-password --gecos '' --uid 1000 cwluser && \
    chown -R cwluser:cwluser /app
USER cwluser

# Set the entrypoint
ENTRYPOINT ["python", "cwl-oscar"]

# Default command shows help
CMD ["--help"] 