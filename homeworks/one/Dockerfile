# 1. Use the official Python 3.12-slim image as the base image
FROM python:3.12-slim

# 2. Set the working directory inside the container to /app
WORKDIR /app

# 3. Copy the requirements.txt file from the host machine to the container's working directory
COPY ./homeworks/one/requirements.txt .

# 4. Install Python dependencies listed in requirements.txt
RUN pip install -r requirements.txt

# 5. Copy the app.py file from the host machine to the container's working directory
COPY ./homeworks/one/app.py .

# 6. Set an environment variable inside the container for the metrics CSV file
ENV METRICS_LOG_FILE=docker_system_inference_metrics.csv

# 7. Expose port 5000 to allow external access to the application running inside the container
EXPOSE 5000

# 8. Specify the command to run the application when the container starts
CMD ["./", "python app.py"]
