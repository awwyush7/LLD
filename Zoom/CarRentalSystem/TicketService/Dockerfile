FROM python:3.10-slim

WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Set PYTHONPATH so imports work
ENV PYTHONPATH=/app

# Expose port
EXPOSE 8001

# Run TicketService
CMD ["uvicorn", "Zoom.CarRentalSystem.TicketService.main:app", "--host", "0.0.0.0", "--port", "8001"]