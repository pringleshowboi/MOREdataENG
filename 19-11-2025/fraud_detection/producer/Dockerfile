# Use a lightweight official Python image
FROM python:3.11-slim

# Set the working directory inside the container
WORKDIR /app

# Copy requirements file first to leverage Docker layer caching
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code into the container
# This is the line that ensures the latest code is included in the IMAGE build
COPY . .

# Command to run the producer script
CMD ["python", "producer.py"]