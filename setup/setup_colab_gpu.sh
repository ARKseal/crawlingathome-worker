#!/bin/bash

export CURRDIR=$(pwd)

cd /tmp
wget https://github.com/protocolbuffers/protobuf/releases/download/v3.17.3/protoc-3.17.3-linux-x86_64.zip
unzip protoc-3.17.3-linux-x86_64.zip
sudo rm /usr/bin/protoc
sudo mv ./bin/protoc /usr/bin
sudo chmod +x /usr/bin/protoc
rm -rf protoc-3.17.3-linux-x86_64.zip bin include readme.txt

cd $CURRDIR
apt-get update && apt-get install -y git build-essential python3.7-dev python3-pip python3.7-venv libtinfo5 libjpeg-dev

rm crawlingathome.py clip_filter.py gpu.py requirements.txt
rm -r crawlingathome_client

git clone "https://github.com/TheoCoombes/crawlingathome" crawlingathome_client

wget https://raw.githubusercontent.com/ARKseal/crawlingathome-worker/multiple-workers/crawlingathome.py
wget https://raw.githubusercontent.com/ARKseal/crawlingathome-worker/multiple-workers/clip_filter.py
wget https://raw.githubusercontent.com/ARKseal/crawlingathome-worker/multiple-workers/gpu.py
wget https://raw.githubusercontent.com/ARKseal/crawlingathome-worker/multiple-workers/requirements/gpu_requirements.txt -O requirements.txt

pip3 install -r crawlingathome_client/requirements.txt --no-cache-dir
pip3 install -r ./requirements.txt --no-cache-dir

yes | pip3 uninstall pillow
CC="cc -mavx2" pip3 install -U --force-reinstall pillow-simd --no-cache-dir
