ARG TIME_WARP=""
FROM ubuntu:26.04 AS base

ENV DEBIAN_FRONTEND=noninteractive
ENV LANG=en_US.UTF-8
RUN apt-get update && \
    apt-get install -y locales && \
    localedef -i en_US -c -f UTF-8 en_US.UTF-8 && \
    apt-get install -y \
        software-properties-common \
        bash \
        python3 \
        python3-pip \
        git \
        curl \
        tar \
        wget && \
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
ARG PYENV_VERSION=""
ENV PYENV_ROOT=/.pyenv
RUN <<EOF bash -euxo pipefail
    git clone https://github.com/pyenv/pyenv.git "$PYENV_ROOT"
    if [[ -n "$PYENV_VERSION" ]]; then
        pushd "$PYENV_ROOT" && git checkout "$PYENV_VERSION" && popd
    fi
    (cd "$PYENV_ROOT" && src/configure && make -C src) || true
EOF
ENV PATH="$PYENV_ROOT/shims:$PYENV_ROOT/bin:$PATH"


FROM base AS base-py-arg
# Install all supported Python versions
ARG PYTHON_VERSIONS="3.15 3.14 3.13 3.12 3.11"
ARG PYTHON_INSTALL_VERSIONS=$PYTHON_VERSIONS


FROM base AS base-py-arg-single-python
# Only install Python 3.11 in time warp mode
ARG PYTHON_VERSIONS="3.11"
ARG PYTHON_INSTALL_VERSIONS=$PYTHON_VERSIONS


FROM base-py-arg${TIME_WARP:+"-single-python"} AS base-py-install
RUN for version in $PYTHON_INSTALL_VERSIONS; do \
        pyenv install $version; \
    done
RUN pyenv rehash
RUN pyenv global $(pyenv versions --bare --skip-aliases | sort --version-sort --reverse)

# Install latest pip and setuptools for each environment
# + tox and tools for starting the tests
# https://pip.pypa.io/en/stable/news/
# Install latest pip
RUN <<EOF bash -euxo pipefail
# https://discuss.python.org/t/avoiding-compromised-certificates-in-pip/109602
# Make sure to only trust system CAs while upgrading pip.
# This ensures a safe bootstrapping distrusting revoked eMudhra certs.
# For pip versions not supporting this kind of CA control, fall back to
# installing a patched pip version with sha256 verification.
# For Python versions not supporting a patched pip version, we are stuck with
# the vulnerability.
PINNED_PIP_WHEEL="pip @ https://files.pythonhosted.org/packages/3f/06/3e4420302363fbb71be3c7f35ce042f54bfca330e3e66b3a2e3c4cca579f/pip-26.2.2-py3-none-any.whl#sha256=fd7f4dfa8190e014013b9f08f3e973f4e4c26ffd179a204895533a7254813a70"
for version in $PYTHON_VERSIONS; do
PIP_CERT=/etc/ssl/certs/ca-certificates.crt \
    PIP_USE_DEPRECATED=legacy-certs \
        python\$version -m pip install -U "\$PINNED_PIP_WHEEL" \
        || python\$version -m pip install -U "\$PINNED_PIP_WHEEL"
    python\$version -m pip install -U pip>=26.2.2
    python\$version -m pip install -U coverage tox
done
EOF

ARG TIME_WARP
ENV DRIVER_TIME_WARP=$TIME_WARP


FROM base-py-install AS backend-timewarp
WORKDIR /home/root/testkit
COPY requirements*.txt .
COPY testkit/backend.py testkit/build.py testkit/_common.py testkit/
RUN sed -i 's|-e \..*$||' requirements*.txt

RUN for version in $PYTHON_VERSIONS; do \
        TEST_BACKEND_VERSION="python$version" python testkit/build.py && \
        python$version -m pip install --force-reinstall neo4j==$TIME_WARP; \
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
RUN env
WORKDIR /home/root/testkit
EXPOSE 9876/tcp
