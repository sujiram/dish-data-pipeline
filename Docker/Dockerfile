# Use official Python base image
FROM python:3.11-slim
  
# Set working directory
WORKDIR /app
  
# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
  
# Copy code and config files
COPY . .

# Environment variables (documented; can be overridden at runtime)
ENV PROJECT_ID=local-abbey-475109-t6
ENV DATASET=dish_dataset
  
# Entry point: allow command-line arguments
ENTRYPOINT ["python", "data_pipeline.py"]