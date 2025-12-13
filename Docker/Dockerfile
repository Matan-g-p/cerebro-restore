FROM apache/airflow:2.9.0

# Copy requirements from the project root (relative to build context)
COPY requirements.txt /requirements.txt

# Install dependencies
RUN pip install --no-cache-dir -r /requirements.txt
