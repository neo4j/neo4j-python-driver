FROM ubuntu:20.04

ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update && \
    apt-get install -y locales && \
    apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* && \
    localedef -i en_US -c -f UTF-8 -A /usr/share/locale/locale.alias en_US.UTF-8 \
 && rm -rf /var/lib/apt/lists/*
ENV LANG en_US.UTF-8

# Using apt-get update alone in a RUN statement causes caching issues and subsequent apt-get install instructions fail.
RUN apt-get --quiet update && apt-get --quiet install -y \
    software-properties-common \
    bash \
    python3 \
    python3-pip \
    git \
    curl \
    tar \
    wget \
 && rm -rf /var/lib/apt/lists/*

# Install Build Tools
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        make build-essential libssl-dev zlib1g-dev libbz2-dev libreadline-dev \
        libsqlite3-dev wget curl llvm libncurses5-dev xz-utils tk-dev \
        libxml2-dev libxmlsec1-dev libffi-dev liblzma-dev \
        ca-certificates && \
    apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Install our own CAs on the image.
# Assumes Linux Debian based image.
COPY CAs/* /usr/local/share/ca-certificates/
RUN update-ca-certificates

# Install pyenv
RUN git clone https://github.com/pyenv/pyenv.git .pyenv
ENV PYENV_ROOT /.pyenv
ENV PATH $PYENV_ROOT/shims:$PYENV_ROOT/bin:$PATH

# Set minimum supported Python version
RUN pyenv install 3.5.10
RUN pyenv rehash
RUN pyenv global 3.5.10

# Set up new Python version, just to be able to load new CA certificates for pip
RUN pyenv install 3.12.4 && \
    pyenv rehash && \
    pyenv global 3.12.4 && \
    python3.12 -m pip install --upgrade pip && \
    python3.12 -m pip install certifi && \
    cp $PYENV_ROOT/versions/3.12.4/lib/python3.12/site-packages/certifi/cacert.pem /tmp/pip-certifi.pem && \
    pyenv global 3.5.10 && \
    pyenv uninstall -f 3.12.4 && \
    pyenv rehash

# Install Latest pip for each environment
# https://pip.pypa.io/en/stable/news/
RUN python -m pip --cert /tmp/pip-certifi.pem install --upgrade pip

# Install Python Testing Tools
RUN python -m pip --cert /tmp/pip-certifi.pem install coverage tox
