FROM ubuntu:20.04 as builder

WORKDIR /crawl

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        libfreetype6-dev \
        libfribidi-dev \
        libharfbuzz-dev \
        libjpeg-dev \
        libjpeg8-dev \
        liblcms2-dev \
        libopenjp2-7-dev \
        libtiff5-dev \
        libtinfo5 \
        libwebp-dev \
        libxcb1-dev \
        python3 \
        python3-dev \
        python3-tk \
        python3-pip \
        python3-venv \
        tcl8.6-dev \
        tk8.6-dev \
        git \
        unzip \
        wget \
        zlib1g-dev \
    \
    && rm -rf /var/lib/apt/lists/* \
    \
    && cd /tmp \
    && wget https://github.com/protocolbuffers/protobuf/releases/download/v3.17.3/protoc-3.17.3-linux-x86_64.zip \
    && unzip protoc-3.17.3-linux-x86_64.zip \
    && mv ./bin/protoc /usr/bin/protoc \
    && rm -rf protoc-3.17.3-linux-x86_64.zip bin include readme.txt \
    && cd /crawl \
    \
    && python3 -m venv venv && . venv/bin/activate \
    && python3 -m pip install -U pip --no-cache-dir \
    && pip3 install wheel --no-cache-dir \
    \
    && pip3 install pycld2 --no-cache-dir || ( \
        pip3 install git+https://github.com/TheoCoombes/pycld2 --no-cache-dir \
    ) \
    \
    && git clone "https://github.com/TheoCoombes/crawlingathome" crawlingathome_client \
    && rm -rf ./crawlingathome_client/.git \
    \
    && wget https://raw.githubusercontent.com/ARKseal/crawlingathome-worker/master/crawlingathome.py \
    && wget https://raw.githubusercontent.com/ARKseal/crawlingathome-worker/master/clip_filter.py \
    && wget https://raw.githubusercontent.com/ARKseal/crawlingathome-worker/master/cpu.py \
    && wget https://raw.githubusercontent.com/ARKseal/crawlingathome-worker/master/requirements/cpu_requirements.txt -O requirements.txt \
    \
    && pip3 install -r crawlingathome_client/requirements.txt --no-cache-dir \
    && pip3 install -r ./requirements.txt --no-cache-dir \
    && rm requirements.txt \
    \
    && yes | pip3 uninstall asks \
    && pip3 install git+https://github.com/rvencu/asks --no-cache-dir

FROM ubuntu:20.04

WORKDIR /crawl

COPY --from=builder /crawl /crawl
COPY --from=builder /usr/bin/protoc /usr/bin/protoc

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        libtinfo5 \
        python3 \
        rsync \
        wget \
    \
    && rm -rf /var/lib/apt/lists/* \
    \
    && chmod +x /usr/bin/protoc

ARG VERSION
ENV CAHVERSION=$VERSION
ENV PYTHONHASHSEED=0
ENV NAME="ARKseal"
ENV PATH="/crawl/venv/bin:$PATH"

CMD nice python3 -u crawlingathome.py --name $NAME --cpu --docker
