# Crawling@Home

> Help us build a billion-scale image-caption dataset by filtering Common Crawl with OpenAI CLIP

## Google Colab
[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/ARKseal/crawlingathome-worker/blob/multiple-workers/notebooks/hybrid-worker.ipynb)

1. Change the value for YOUR_NICKNAME_FOR_THE_LEADERBOARD and make sure you are connected to a gpu runtime to maximize efficiency.
2. Then just run all (`Ctrl+F9`) to install dependencies and start Crawling!

### Other options

- [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/ARKseal/crawlingathome-worker/blob/multiple-workers/notebooks/cpu-worker.ipynb) 
    * If you want to run a cpu only worker (don't use a gpu runtime)
- [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/ARKseal/crawlingathome-worker/blob/multiple-workers/notebooks/gpu-worker.ipynb)
    * If you want to run a gpu only worker (please use a gpu runtime)

## Docker file
1. Get the docker image using `docker pull arkseal/cah-worker`
2. Run docker image using `docker run --shm-size=1g -d arkseal/cah-worker`
    - add `-e NAME={nickname}` to specify display name
##### You can use this one liner: `docker pull arkseal/cah-worker && docker run --shm-size=1g -d arkseal/cah-worker`
- add `-e NAME={nickname}` to specify display name

## Setup
1. `wget https://raw.githubusercontent.com/ARKseal/crawlingathome-worker/multiple-workers/setup/setup_hybrid.sh`
2. `bash setup.sh`, to install dependencies.
3. `export PYTHONHASHCODE=0 && python3 crawlingathome.py`, to start Crawling!
    * use `--name {nickname}` to specify your display name
### Other Options
- CPU Only Worker:
    1. `wget https://raw.githubusercontent.com/ARKseal/crawlingathome-worker/multiple-workers/setup/setup_cpu.sh`
    2. `bash setup.sh`, to install dependencies.
    3. `export PYTHONHASHCODE=0 && python3 crawlingathome.py --cpu`, to start Crawling!
        * use `--name {nickname}` to specify your display name
- GPU Only Worker:
    1. `wget https://raw.githubusercontent.com/ARKseal/crawlingathome-worker/multiple-workers/setup/setup_gpu.sh`
    2. `bash setup.sh`, to install dependencies.
    3. `export PYTHONHASHCODE=0 && python3 crawlingathome.py --gpu`, to start Crawling!
        * use `--name {nickname}` to specify your display name

## Droplet Setup
1. use `cloud-config.yaml` script to init the droplet
2. ssh with this command `ssh -oIdentitiesOnly=yes -i~/.ssh/id_cah crawl@{your-droplet-ip}}`
3. check the script by running `tail -f crawl.log`

## TODO
- [x] Save image embedding 
- [x] Convert images to tfrecords
- [x] Upload to google drive
- [x] Prevent corrupt image to be processed
- [x] Shard of chunk (it needs to read all WAT file which will be bad for low ram server)
- [x] Crawling@Home integration
- [x] Verify output
