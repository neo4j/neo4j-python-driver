FROM python:3.13

WORKDIR /driver

COPY . /driver

# Install dependencies
RUN pip install -U pip && \
    pip install --group benchkit && \
    pip install .

ENTRYPOINT ["python", "-m", "benchkit"]
