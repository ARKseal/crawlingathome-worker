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
python3 -m venv venv && . venv/bin/activate

rm blocklist-domain.txt failed-domains.txt 5Mduplicates.txt crawlingathome.py clip_filter.py requirements.txt
rm -r crawlingathome_client

git clone "https://github.com/TheoCoombes/crawlingathome" crawlingathome_client

wget https://raw.githubusercontent.com/ARKseal/crawlingathome-worker/update-script/crawlingathome.py
wget https://raw.githubusercontent.com/ARKseal/crawlingathome-worker/update-script/requirements.txt
wget https://raw.githubusercontent.com/ARKseal/crawlingathome-worker/update-script/clip_filter.py

wget https://raw.githubusercontent.com/rvencu/crawlingathome-gpu-hcloud/main/blocklists/blocklist-domain.txt
wget https://raw.githubusercontent.com/rvencu/crawlingathome-gpu-hcloud/main/blocklists/failed-domains.txt
wget https://raw.githubusercontent.com/rvencu/crawlingathome-gpu-hcloud/main/blocklists/5Mduplicates.txt

pip3 install wheel --no-cache-dir

pip3 install torch==1.7.1$1 torchvision==0.8.2$1 -f https://download.pytorch.org/whl/torch_stable.html --no-cache-dir

pip3 install -r crawlingathome_client/requirements.txt --no-cache-dir
pip3 install -r ./requirements.txt --no-cache-dir

pip3 install datasets ftfy pandas pycld2 regex tfr_image tractor trio ujson --no-cache-dir
pip3 install tensorflow --no-cache-dir

pip3 install clip-anytorch --no-cache-dir

yes | pip3 uninstall pillow
CC="cc -mavx2" pip3 install -U --force-reinstall pillow-simd --no-cache-dir

yes | pip3 uninstall asks
pip3 install git+https://github.com/rvencu/asks --no-cache-dir
