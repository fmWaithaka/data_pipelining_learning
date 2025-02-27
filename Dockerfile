FROM python:3.11.5

WORKDIR /app

# Copy everything first to leverage Docker caching
COPY . /app

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Entry point
CMD ["python", "app.py", "dev", "all"]
