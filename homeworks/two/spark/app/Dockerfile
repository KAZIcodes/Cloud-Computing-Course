FROM bde2020/spark-submit:3.3.0-hadoop3.3

COPY template.sh /

# Set working directory
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt /app/
RUN pip3 install -r requirements.txt

# Copy the entire source code
COPY . /app

# Set the Spark application path correctly (assuming your main script is code.py)
ENV SPARK_APPLICATION_PYTHON_LOCATION="/app/app.py"

CMD ["/bin/bash", "/template.sh"]
