ARG TIME_WARP=""
FROM ubuntu:26.04 AS base

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && \
    apt-get install -y locales && \
    localedef -i en_US -c -f UTF-8 en_US.UTF-8 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
ENV LANG=en_US.UTF-8

# Using apt-get update alone in a RUN statement causes caching issues and subsequent apt-get install instructions fail.
RUN apt-get update && \
    apt-get install -y \
        software-properties-common \
        bash \
        python3 \
        python3-pip \
        git \
        curl \
        tar \
        wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Install Build Tools
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        make \
        build-essential \
        libssl-dev \
        zlib1g-dev \
        libbz2-dev \
        libreadline-dev\
        libsqlite3-dev \
        wget \
        curl \
        llvm \
        libncurses5-dev \
        xz-utils \
        tk-dev\
        libxml2-dev \
        libxmlsec1-dev \
        libffi-dev \
        liblzma-dev\
        ca-certificates && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Install pyenv
RUN git clone https://github.com/pyenv/pyenv.git .pyenv
ENV PYENV_ROOT=/.pyenv
ENV PATH="$PYENV_ROOT/shims:$PYENV_ROOT/bin:$PATH"
ENV PIP_NO_CACHE_DIR=1


FROM base AS base-py-arg
# Install all supported Python versions
ARG PYTHON_VERSIONS="3.15 3.14 3.13 3.12 3.11 3.10"
ARG PYTHON_INSTALL_VERSIONS="3.15.0rc2 3.14 3.13 3.12 3.11 3.10"

# [bump-pyarrow] search tag when bumping pyarrow
# Installing pyarrow lib until pre-built wheel for Python 3.15 exists
# https://github.com/apache/arrow/issues/48172
RUN apt update && \
    apt install -y -V wget lsb-release cmake gcc && \
    distro_name=$(lsb_release --id --short | tr 'A-Z' 'a-z') && \
    code_name=$(lsb_release --codename --short) && \
    wget https://apache.jfrog.io/artifactory/arrow/${distro_name}/apache-arrow-apt-source-latest-${code_name}.deb && \
    apt install -y -V ./apache-arrow-apt-source-latest-${code_name}.deb && \
    apt update && \
    apt install -y -V libarrow-dev libarrow-dataset-dev libarrow-flight-dev libarrow-acero-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* ./apache-arrow-apt-source-latest-${code_name}.deb
ENV PYARROW_WITH_CUDA=OFF
ENV PYARROW_WITH_GANDIVA=OFF


FROM base AS base-py-arg-single-python
# Only install Python 3.10 in time warp mode
ARG PYTHON_VERSIONS="3.10"
ARG PYTHON_INSTALL_VERSIONS=PYTHON_VERSIONS


FROM base-py-arg${TIME_WARP:+"-single-python"} AS base-py-install

RUN for version in $PYTHON_INSTALL_VERSIONS; do \
        pyenv install $version; \
    done
RUN pyenv rehash
RUN pyenv global $(pyenv versions --bare --skip-aliases | sort --version-sort --reverse)

# Install Latest pip and setuptools for each environment
# + tox and tools for starting the tests
# https://pip.pypa.io/en/stable/news/
RUN for version in $PYTHON_VERSIONS; do \
        python$version -m pip install -U pip; \
    done

ARG TIME_WARP
ENV DRIVER_TIME_WARP=$TIME_WARP


FROM base-py-install AS backend-timewarp
WORKDIR /home/root/testkit

COPY testkit/backend.py testkit/build.py testkit/_common.py testkit/
COPY pyproject.toml .

RUN for version in $PYTHON_VERSIONS; do \
        TEST_BACKEND_VERSION="python$version" python testkit/build.py && \
        python$version -m pip install --force-reinstall neo4j==${TIME_WARP}; \
    done
COPY testkitbackend ./testkitbackend
ENTRYPOINT ["python", "testkit/backend.py"]


FROM base-py-install AS backend

# Install our own CAs on the image.
# Assumes Linux Debian based image.
COPY CAs/* /usr/local/share/ca-certificates/
# Store custom CAs somewhere where the backend can find them later.
COPY CustomCAs/* /usr/local/share/custom-ca-certificates/
RUN update-ca-certificates


FROM backend${TIME_WARP:+"-timewarp"} AS final
WORKDIR /home/root/testkit
EXPOSE 9876/tcp
