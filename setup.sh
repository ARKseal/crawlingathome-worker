#!/bin/bash

sudo apt-get update && sudo apt-get install -y git build-essential python3-dev python3-pip python3-venv libjpeg-dev
python3 -m venv venv && . venv/bin/activate

rm blocklist-domain.txt failed-domains.txt 5Mduplicates.txt crawlingathome.py clip_filter.py requirements.txt
rm -r crawlingathome_client

git clone "https://github.com/TheoCoombes/crawlingathome" crawlingathome_client

wget https://raw.githubusercontent.com/ARKseal/crawlingathome-worker/master/crawlingathome.py
wget https://raw.githubusercontent.com/ARKseal/crawlingathome-worker/master/requirements.txt
wget https://raw.githubusercontent.com/ARKseal/crawlingathome-worker/master/clip_filter.py

wget https://raw.githubusercontent.com/rvencu/crawlingathome-gpu-hcloud/main/blocklists/blocklist-domain.txt
wget https://raw.githubusercontent.com/rvencu/crawlingathome-gpu-hcloud/main/blocklists/failed-domains.txt
wget https://raw.githubusercontent.com/rvencu/crawlingathome-gpu-hcloud/main/blocklists/5Mduplicates.txt

pip3 install wheel --no-cache-dir

pip3 install torch==1.7.1 torchvision==0.8.2 -f https://download.pytorch.org/whl/torch_stable.html --no-cache-dir

pip3 install -r crawlingathome_client/requirements.txt --no-cache-dir
pip3 install -r ./requirements.txt --no-cache-dir

pip3 install datasets ftfy pandas pycld2 regex tfr_image tractor trio ujson --no-cache-dir

pip3 install tensorflow --no-cache-dir

pip3 install clip-anytorch --no-cache-dir

pip3 install -U --force-reinstall pdbpp --no-cache-dir
pip3 install --force-reinstall msgpack==1.0.1 --no-cache-dir

yes | pip3 uninstall pillow
CC="cc -mavx2" pip3 install -U --force-reinstall pillow-simd --no-cache-dir

yes | pip3 uninstall asks
pip3 install git+https://github.com/rvencu/asks --no-cache-dir
