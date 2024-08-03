# src/backend/Dockerfile
FROM python:3.11-slim

WORKDIR /app

# Copy only the requirements file
COPY src/backend/requirements.txt .

# Install the requirements
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the backend source code
COPY src/backend/ .

CMD ["python", "app.py"]
