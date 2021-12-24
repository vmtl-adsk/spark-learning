# Install FastAPI Service
FROM python:3.9.1-buster
RUN pip install --upgrade pip
RUN pip install poetry
COPY poetry.lock pyproject.toml /opt/spark-learning/fastapi/
WORKDIR /opt/spark-learning/fastapi
COPY . .
RUN poetry config virtualenvs.create false && poetry install