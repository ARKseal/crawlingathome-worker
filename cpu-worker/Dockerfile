FROM ubuntu:20.04 as builder

WORKDIR /crawl

# protobuf version
ARG PV=3.17.3
ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        git \
        libfreetype6-dev \
        libfribidi-dev \
        libharfbuzz-dev \
        libjpeg-dev \
        libjpeg8-dev \
        libopenjp2-7-dev \
        libtiff5-dev \
        libtinfo5 \
        libwebp-dev \
        libxcb1-dev \
        pkg-config \
        python3 \
        python3-dev \
        python3-numpy \
        python3-scipy \
        python3-pip \
        python3-setuptools \
        python3-tk \
        python3-venv \
        sudo \
        tcl8.6-dev \
        tk8.6-dev \
        unzip \
        wget \
        zlib1g-dev \
    \
    && rm -rf /var/lib/apt/lists/* \
    \
    && cd /tmp \
    && wget https://github.com/protocolbuffers/protobuf/releases/download/v$PV/protoc-$PV-linux-x86_64.zip \
    && unzip protoc-$PV-linux-x86_64.zip \
    && mv ./bin/protoc /usr/bin/protoc \
    \
    && cd /crawl \
    && rm -rf /tmp \
    \
    && python3 -m venv venv && . venv/bin/activate \
    && python3 -m pip install -U pip --no-cache-dir \
    && pip3 install wheel --no-cache-dir \
    \
    && (pip3 install pycld2 --no-cache-dir || ( \
        pip3 install git+https://github.com/TheoCoombes/pycld2 --no-cache-dir \
    )) \
    \
    && git clone "https://github.com/TheoCoombes/crawlingathome" crawlingathome_client \
    && rm -rf ./crawlingathome_client/.git \
    \
    && wget https://raw.githubusercontent.com/ARKseal/crawlingathome-worker/master/crawlingathome.py \
    && wget https://raw.githubusercontent.com/ARKseal/crawlingathome-worker/master/cpu.py \
    && wget https://raw.githubusercontent.com/ARKseal/crawlingathome-worker/master/requirements/cpu_requirements.txt -O requirements.txt \
    \
    && pip3 install -r crawlingathome_client/requirements.txt --no-cache-dir \
    && pip3 install -r ./requirements.txt --no-cache-dir \
    && rm requirements.txt

FROM ubuntu:20.04

WORKDIR /crawl

COPY --from=builder /crawl /crawl
COPY --from=builder /usr/bin/protoc /usr/bin/protoc

ARG VERSION
ENV CAHVERSION=$VERSION
ENV PYTHONHASHSEED=0
ENV NAME="ARKseal"
ENV PATH="/crawl/venv/bin:$PATH"

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        libffi-dev \
        libssl-dev \
        libtinfo5 \
        python3 \
        rsync \
    \
    && rm -rf /var/lib/apt/lists/* \
    \
    && chmod +x /usr/bin/protoc

CMD nice python3 -u crawlingathome.py --name $NAME --cpu --docker
