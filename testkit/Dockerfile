FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && \
    apt-get install -y locales && \
    apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* && \
    localedef -i en_US -c -f UTF-8 -A /usr/share/locale/locale.alias en_US.UTF-8 \
 && rm -rf /var/lib/apt/lists/*
ENV LANG=en_US.UTF-8

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
# Store custom CAs somewhere where the backend can find them later.
COPY CustomCAs/* /usr/local/share/custom-ca-certificates/
RUN update-ca-certificates

# Install pyenv
RUN git clone https://github.com/pyenv/pyenv.git .pyenv
ENV PYENV_ROOT=/.pyenv
ENV PATH="$PYENV_ROOT/shims:$PYENV_ROOT/bin:$PATH"

# Setup python version
ENV PYTHON_VERSIONS="3.12 3.11 3.10 3.9 3.8 3.7"

RUN for version in $PYTHON_VERSIONS; do \
        pyenv install $version; \
    done
RUN pyenv rehash
RUN pyenv global $(pyenv versions --bare --skip-aliases | sort --version-sort --reverse)

# Install Latest pip and setuptools for each environment
# + tox and tools for starting the tests
# https://pip.pypa.io/en/stable/news/
RUN for version in $PYTHON_VERSIONS; do \
        python$version -m pip install -U pip && \
        python$version -m pip install -U coverage tox; \
    done

# Install Rust
RUN curl --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --default-toolchain stable -y --profile minimal
ENV PATH="/root/.cargo/bin:$PATH"
