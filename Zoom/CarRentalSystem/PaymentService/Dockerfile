FROM python:3.10-slim

WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Set PYTHONPATH so imports work
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Run PaymentService
CMD ["python", "-m", "Zoom.CarRentalSystem.PaymentService.main"]