# Use Python 3.12
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install uv for dependency management
RUN pip install uv

# Copy dependency files from chronon-ui directory
COPY chronon-ui/pyproject.toml chronon-ui/uv.lock /app/

# Install dependencies using uv
RUN uv sync --frozen

# Copy only the server application code
COPY chronon-ui/server /app/server

# Copy chronon config from local_deployment/app
COPY app /app/server/chronon_config

# Expose the port the app runs on
EXPOSE 8005

# Start the FastAPI server with hot reload (stay in /app directory)
CMD ["uv", "run", "fastapi", "dev", "server/main.py", "--host", "0.0.0.0", "--port", "8005"]

