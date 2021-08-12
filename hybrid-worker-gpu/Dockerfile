FROM nvidia/cuda:11.0.3-devel-ubuntu20.04 as builder

WORKDIR /crawl

RUN apt-get update && apt-get install -y git libjpeg-dev libopenjp2-7-dev libtiff5-dev libtinfo5 python3 python3-dev python3-pip python3-venv unzip wget zlib1g-dev --no-install-recommends \
    && rm -rf /var/lib/apt/lists/* \
    \
    && cd /tmp \
    && wget https://github.com/protocolbuffers/protobuf/releases/download/v3.17.3/protoc-3.17.3-linux-x86_64.zip \
    && unzip protoc-3.17.3-linux-x86_64.zip \
    && mv ./bin/protoc /usr/bin/protoc \
    && rm -rf protoc-3.17.3-linux-x86_64.zip bin include readme.txt \
    && cd /crawl \
    \
    && git clone "https://github.com/TheoCoombes/crawlingathome" crawlingathome_client \
    && rm -rf ./crawlingathome_client/.git \
    \
    && wget https://raw.githubusercontent.com/ARKseal/crawlingathome-worker/master/crawlingathome.py \
    && wget https://raw.githubusercontent.com/ARKseal/crawlingathome-worker/master/clip_filter.py \
    && wget https://raw.githubusercontent.com/ARKseal/crawlingathome-worker/master/hybrid.py \
    && wget https://raw.githubusercontent.com/ARKseal/crawlingathome-worker/master/requirements/hybrid_requirements.txt -O requirements.txt \
    \
    && python3 -m venv venv && . venv/bin/activate \
    && python3 -m pip install -U pip --no-cache-dir \
    \
    && pip3 install wheel --no-cache-dir \
    \
    && pip3 install torch==1.7.1+cu110 torchvision==0.8.2+cu110 -f https://download.pytorch.org/whl/torch_stable.html --no-cache-dir \
    \
    && pip3 install -r crawlingathome_client/requirements.txt --no-cache-dir \
    && pip3 install -r ./requirements.txt --no-cache-dir \
    \
    && yes | pip3 uninstall asks \
    && pip3 install git+https://github.com/rvencu/asks --no-cache-dir

FROM nvidia/cuda:11.0.3-base-ubuntu20.04

WORKDIR /crawl

COPY --from=builder /crawl /crawl
COPY --from=builder /usr/bin/protoc /usr/bin/protoc

RUN apt-get update && apt-get install -y libjpeg-dev libopenjp2-7-dev libtiff5-dev libtinfo5 python3 rsync zlib1g-dev --no-install-recommends \
    && rm -rf /var/lib/apt/lists/* \
    \
    && chmod +x /usr/bin/protoc

ENV PYTHONHASHSEED=0
ENV NAME="ARKseal"
ENV PATH="/crawl/venv/bin:$PATH"

CMD nice python3 -u crawlingathome.py --name $NAME
