FROM python:3.12

WORKDIR /driver

COPY . /driver

# Install dependencies
RUN pip install -U pip && \
    pip install -Ur requirements-dev.txt

ENTRYPOINT ["python", "-m", "benchkit"]
