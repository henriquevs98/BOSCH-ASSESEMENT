# Use an official Python runtime as a parent image
FROM python:3.9-slim-buster

# Set the working directory to /app
WORKDIR /app

# Copy the requirements file into the container at /app
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --trusted-host pypi.python.org -r requirements.txt

# Copy the rest of the application code into the container at /app
COPY ./app /app

# Expose port 80 to the outside world
EXPOSE 80

# Start the application
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "80"]