FROM python:3.11-slim

WORKDIR /opt/app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python packages (include duckdb for in-app queries)
RUN pip install --no-cache-dir \
    streamlit==1.32.0 \
    polars==0.20.0 \
    plotly==5.18.0 \
    duckdb==0.10.2 \
    pandas==2.1.4

# Expose Streamlit port
EXPOSE 8501

# Directly launch Streamlit using absolute path
CMD ["streamlit", "run", "/opt/app/streamlit_app.py", "--server.port=8501", "--server.address=0.0.0.0"]
