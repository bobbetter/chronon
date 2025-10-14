# Use Python 3.12
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install uv for dependency management
RUN pip install uv

# Copy dependency files from repo root
COPY pyproject.toml uv.lock /app/

# Install dependencies using uv
RUN uv sync --frozen

# Copy only the server application code from repo root
COPY server /app/server

# Set working directory to server app
WORKDIR /app/server

# Expose the port the app runs on
EXPOSE 8005

# Start the FastAPI server with hot reload
CMD ["uv", "run", "fastapi", "dev", "main.py", "--host", "0.0.0.0", "--port", "8005"]

