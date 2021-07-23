FROM python:3.8

WORKDIR /crawl

RUN apt-get update && apt-get upgrade -y && apt-get install -y git build-essential libtinfo5 libjpeg-dev python3-dev python3-pip python3-venv rsync

RUN cd /tmp && \
    wget https://github.com/protocolbuffers/protobuf/releases/download/v3.17.3/protoc-3.17.3-linux-x86_64.zip && \
    unzip protoc-3.17.3-linux-x86_64.zip && \
    mv ./bin/protoc /usr/bin && \
    chmod +x /usr/bin/protoc && \
    rm -rf protoc-3.17.3-linux-x86_64.zip bin include readme.txt && \
    cd /crawl && \
    \
    python3 -m venv venv && . venv/bin/activate && \
    python3 -m pip install -U pip && \
    \
    git clone "https://github.com/TheoCoombes/crawlingathome" crawlingathome_client && \
    \
    wget https://raw.githubusercontent.com/ARKseal/crawlingathome-worker/master/crawlingathome.py && \
    wget https://raw.githubusercontent.com/ARKseal/crawlingathome-worker/master/clip_filter.py && \
    wget https://raw.githubusercontent.com/ARKseal/crawlingathome-worker/master/hybrid.py && \
    wget https://raw.githubusercontent.com/ARKseal/crawlingathome-worker/master/requirements/hybrid_requirements.txt -O requirements.txt && \
    \
    pip3 install wheel --no-cache-dir && \
    \
    pip3 install torch==1.7.1+cpu torchvision==0.8.2+cpu -f https://download.pytorch.org/whl/torch_stable.html --no-cache-dir && \
    \
    pip3 install -r crawlingathome_client/requirements.txt --no-cache-dir && \
    pip3 install -r ./requirements.txt --no-cache-dir && \
    \
    yes | pip3 uninstall pillow && \
    CC="cc -mavx2" pip3 install -U --force-reinstall pillow-simd --no-cache-dir && \
    \
    yes | pip3 uninstall asks && \
    pip3 install git+https://github.com/rvencu/asks --no-cache-dir

ENV PYTHONHASHSEED=0
ENV NAME="ARKseal"

CMD . venv/bin/activate && nice python3 -u crawlingathome.py --name $NAME
