import multiprocessing as mp
import os
import shutil
import time
import traceback
import warnings
from glob import glob
from pathlib import Path

import pandas
import ujson
from PIL import ImageFile

import crawlingathome_client as cah

ImageFile.LOAD_TRUNCATED_IMAGES = True  # https://stackoverflow.com/a/47958486

warnings.filterwarnings("ignore")


def upload(source: str, client_type: str):
    client_type = client_type.upper()
    target = 'gpujobs' if client_type == 'CPU' else 'CAH'
    options = '-rsh' if client_type == 'CPU' else '-zh'
    return os.system(f'rsync {options} {source} archiveteam@88.198.2.17::{target}')


def download(url, name):
    client = cah.init(
        url=url, nickname=name, type='gpu'
    )

    while client.jobCount() > 0:
        try:
            if not client.isAlive():
                client = cah.init(
                    url=url, nickname=name, type='gpu'
                )

            start_dl = time.time()

            client.newJob()
            client.downloadShard()

            end_dl = time.time()

            uid = client.shard.split('rsync', 1)[-1].strip()
            if len(glob(f'{uid}/*.csv')) == 0:
                print(f'[crawling@home] Marking job {uid} as invalid')
                client.invalidURL()
            for file in glob(f"{uid}/*_parsed.csv") + glob(f"{uid}/*_unfiltered.csv"):
                shutil.move(file, 'stats/')

            print(f'[crawling@home] Downloaded job {uid} in {end_dl-start_dl}')

            with open(f'{uid}/client.json', 'w') as f:
                ujson.dump(client.dump(), f)
        except cah.errors.InvalidURLError:
            pass
        except Exception as ex:
            print(f"[crawling@home] ERROR: {ex}")
            if client.isAlive():
                try:
                    client.log('Error, restarting job')
                except:
                    print("[crawling@home] Couldn't log to client:")
    try:
        if client.isAlive():
            client.bye()
    except:
        pass


pool = None


def downloader(url, name):
    n_processes = mp.cpu_count()
    pool = mp.Pool(n_processes)
    pool.starmap_async(download, [(url, name) for _ in range(4)])


def main(name, url, debug, isnotebook):

    print('[crawling@home] loading clip')
    from clip_filter import run_inference
    print('\n[crawling@home] clip loaded\n')

    def _worker(client_dict):
        while True:
            try:
                client = cah.load( **client_dict )
                output_folder = f"./{client.shard.split('rsync', 1)[-1].strip()}/"

                start = time.time()

                first_sample_id = int(client.start_id)
                last_sample_id = int(client.end_id)
                shard_of_chunk = client.shard_piece

                out_fname = \
                    f"FIRST_SAMPLE_ID_IN_SHARD_{first_sample_id}_LAST_SAMPLE_ID_IN_SHARD_{last_sample_id}_{shard_of_chunk}"
                print(
                    f"[crawling@home] shard identification {out_fname}"
                )  # in case test fails, we need to remove bad data

                dlparse_df = pandas.read_csv(
                    f'{output_folder}{out_fname}.csv', sep="|")

                client.log("Dropping NSFW keywords")

                filtered_df_len = run_inference(
                    dlparse_df, output_folder, out_fname)

                client.log("Uploading Results")

                upload(f'{output_folder}/*{out_fname}*', client.type)

                client.completeJob(filtered_df_len)
                end = time.time()
                print(
                    f"[crawling@home] job completed in {round(end - start)} seconds")
                print(
                    f"[crawling@home] job efficiency {filtered_df_len / (end - start)} pairs/sec")
                shutil.rmtree(output_folder)
                break
            except Exception as ex:
                print(f"[crawling@home] ERROR: {ex}")
                if debug:
                    traceback.print_exc()
                if client.isAlive():
                    try:
                        client.log('Error, restarting job')
                    except:
                        print("[crawling@home] Couldn't log to client")
        try:
            if client.isAlive():
                client.bye()
        except:
            pass
        pass

    while True:
        try:
            for client_dump in glob('./*/client.json'):
                with open(client_dump, 'r') as f:
                    client_dict = ujson.load(f)
                    uid = client_dict['shard'].split('rsync', 1)[-1].strip()
                    _worker(client_dict)
        except KeyboardInterrupt:
            print('[crawling@home] stopping worker')
            if hasattr(pool, 'close'):
                pool.close()
            print('[crawling@home] stopped worker, cleaning workspace')
            break
        except Exception as ex:
            print(f'[crawling@home] ERROR: {ex}')
            if debug:
                traceback.print_exc()

    for client in glob('./*/client.json'):
        with open(client) as f:
            client = cah.load(**ujson.load(f))
            client.bye()
    for folder in glob('./*'):
        if 'crawlingathome_client' in folder or 'venv' in folder or 'stats' in folder:
            continue
        path = Path(folder)
        if path.is_dir():
            shutil.rmtree(folder)
    print('[crawling@home] cleaned workspace')
