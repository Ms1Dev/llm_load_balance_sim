FROM python:3.12-slim
WORKDIR /app
COPY vLLM/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY vLLM/main.py .
COPY utils /app/utils
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8002"]
