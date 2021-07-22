#!/bin/bash

apt-get update && apt-get install -y git build-essential python3.7-dev python3-pip python3.7-venv libtinfo5 libjpeg-dev rsync
python3 -m venv venv && . venv/bin/activate

rm crawlingathome.py clip_filter.py cpu.py requirements.txt
rm -r blocklists
rm -r crawlingathome_client

git clone "https://github.com/TheoCoombes/crawlingathome" crawlingathome_client

wget https://raw.githubusercontent.com/ARKseal/crawlingathome-worker/multiple-workers/crawlingathome.py
wget https://raw.githubusercontent.com/ARKseal/crawlingathome-worker/multiple-workers/clip_filter.py
wget https://raw.githubusercontent.com/ARKseal/crawlingathome-worker/multiple-workers/cpu.py
wget https://raw.githubusercontent.com/ARKseal/crawlingathome-worker/multiple-workers/requirements/cpu_requirements.txt -O requirements.txt

pip3 install wheel --no-cache-dir

pip3 install -r crawlingathome_client/requirements.txt --no-cache-dir
pip3 install -r ./requirements.txt --no-cache-dir

yes | pip3 uninstall pillow
CC="cc -mavx2" pip3 install -U --force-reinstall pillow-simd --no-cache-dir

yes | pip3 uninstall asks
pip3 install git+https://github.com/rvencu/asks --no-cache-dir
