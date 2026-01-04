FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python packages
RUN pip install --no-cache-dir \
    streamlit==1.32.0 \
    duckdb==0.10.0 \
    polars==0.20.0 \
    plotly==5.18.0

# Expose Streamlit port
EXPOSE 8501

# Run Streamlit (app.py will be available via volume mount)
CMD ["streamlit", "run", "app/app.py", "--server.port=8501", "--server.address=0.0.0.0"]
