FROM python:3.12-slim
RUN pip install --no-cache-dir "psycopg[binary]==3.2.*"
COPY generator.py /app/generator.py
CMD ["python", "-u", "/app/generator.py"]
