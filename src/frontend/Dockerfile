# src/frontend/Dockerfile
FROM python:3.11-slim

WORKDIR /app

# Copy only the requirements file
COPY src/frontend/requirements.txt .

# Install the requirements
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the frontend source code
COPY src/frontend/ .

CMD ["python", "app.py"]
