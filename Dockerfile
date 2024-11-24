# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any necessary dependencies
RUN pip3 install --upgrade pip
RUN pip3 install -r requirements.txt

# Set environment variables (Optional)
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Expose any ports that the application will run on (Optional)
EXPOSE 8080

# Set the command to run the application
CMD ["python3", "src/__main__.py"]
