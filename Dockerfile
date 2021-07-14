FROM python:3.8

WORKDIR /crawl

RUN apt-get update && apt-get install -y git build-essential python3-dev python3-pip python3-venv libtinfo5 libjpeg-dev 

RUN git clone "https://github.com/TheoCoombes/crawlingathome" crawlingathome_client && \
wget https://raw.githubusercontent.com/ARKseal/crawlingathome-worker/master/crawlingathome.py && \
wget httpus://raw.githubusercontent.com/ARKseal/crawlingathome-worker/master/requirements.txt && \
wget https://raw.githubusercontent.com/ARKseal/crawlingathome-worker/master/clip_filter.py && \
\
RUN wget https://raw.githubusercontent.com/rvencu/crawlingathome-gpu-hcloud/main/blocklists/blocklist-domain.txt && \
wget https://raw.githubusercontent.com/rvencu/crawlingathome-gpu-hcloud/main/blocklists/failed-domains.txt && \
wget https://raw.githubusercontent.com/rvencu/crawlingathome-gpu-hcloud/main/blocklists/5Mduplicates.txt

RUN python3 -m venv venv && . venv/bin/activate && \
pip3 install wheel --no-cache-dir && \
\
pip3 install torch==1.7.1+cpu torchvision==0.8.2+cpu -f https://download.pytorch.org/whl/torch_stable.html --no-cache-dir && \
\
pip3 install -r crawlingathome_client/requirements.txt --no-cache-dir && \
pip3 install -r ./requirements.txt --no-cache-dir && \
\
pip3 install tensorflow --no-cache-dir && \
\
pip3 install clip-anytorch --no-cache-dir && \
\
yes | pip3 uninstall pillow && \
CC="cc -mavx2" pip3 install -U --force-reinstall pillow-simd --no-cache-dir && \
\
yes | pip3 uninstall asks && \
pip3 install git+https://github.com/rvencu/asks --no-cache-dir && \
\
yes | pip3 uninstall protobuf && \
pip3 install protobuf==3.9.2

ENV PYTHONHASHSEED=0
ENV NAME="ARKseal"

CMD touch ./crawl.log && . venv/bin/activate && nice python3 -u crawlingathome.py --name $NAME | tee ./crawl.log
