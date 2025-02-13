# Use official Python runtime as the base image
FROM python:3.9-slim

# Set working directory in the container
WORKDIR /app

# Copy requirements first to leverage Docker cache
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the bot script
COPY main/ID_detector_bot.py .

# Run the bot
CMD ["python", "ID_detector_bot.py"] 