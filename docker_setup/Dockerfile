FROM python:3.9.1

RUN pip install pandas

WORKDIR /app

COPY test_pipeline.py test_pipeline.py

ENTRYPOINT ["python", "test_pipeline.py"]