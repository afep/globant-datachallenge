# Use the official Python 3.9 slim image as a base
FROM python:3.9-slim

# Set the working directory inside the container
WORKDIR /src

# Copy the files from requirements.txt to the container
COPY requirements.txt .

# Install the necessary dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the project files to the container
COPY . .

# Expose the port the app runs on
EXPOSE 5000

# Set environment variables for Flask
ENV FLASK_APP=app.py
ENV FLASK_ENV=production

# Command to run the application
CMD ["flask", "run", "--host=0.0.0.0", "--port=5000"]