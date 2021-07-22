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


def download(url, name, debug, isnotebook):
    while True:
        try:
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
            if isnotebook:
                time.sleep(25)
        except (cah.errors.InvalidURLError, cah.errors.ZeroJobError):
            try:
                if client.isAlive():
                    client.bye()
            except:
                pass
            time.sleep(5)
        except KeyboardInterrupt:
            if client.isAlive():
                client.bye()
        except Exception as ex:
            print(f"[crawling@home] DLERROR: {ex}")
            if debug:
                traceback.print_exc()
            if client.isAlive():
                try:
                    client.log('Error, restarting job')
                except:
                    print("[crawling@home] Couldn't log to client:")


pool = None


def downloader(name, url, debug, isnotebook, workers):
    pool = mp.Pool(workers)
    pool.starmap_async(
        download, [(url, name, debug, isnotebook) for _ in range(1)])


def main(name, url, debug, isnotebook, workers):

    if not Path("stats").exists():
        os.mkdir("stats")

    print('[crawling@home] loading clip')
    from clip_filter import run_inference
    print('\n[crawling@home] clip loaded\n')

    def _worker(client_dict):
        while True:
            try:
                client = cah.load(**client_dict)
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
                dlparse_df["PATH"] = dlparse_df.apply(
                    lambda x: output_folder + x["PATH"].strip("save/"), axis=1)

                client.log("Dropping NSFW keywords")

                filtered_df_len = run_inference(
                    dlparse_df, output_folder, out_fname)

                client.log("Uploading Results")

                upload(f'{output_folder}/*{out_fname}*', client.type)

                client.completeJob(filtered_df_len)
                client.bye()
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

    print('start download')
    downloader(name, url, debug, isnotebook, workers)
    print('download started')

    while True:
        try:
            for client_dump in glob('./*/client.json'):
                with open(client_dump, 'r') as f:
                    print(client_dump)
                    try:
                        client_dict = ujson.load(f)
                    except ValueError:
                        continue
                    print(client_dict)
                    _worker(client_dict)
                continue
        except KeyboardInterrupt:
            print('[crawling@home] stopping worker')
            if hasattr(pool, 'terminate'):
                print('terminating pool')
                pool.terminate()
                print('joining pool')
                pool.join()
            print('[crawling@home] stopped worker, cleaning workspace')
            break
        except Exception as ex:
            try:
                print(f'[crawling@home] ERROR: {ex}')
                if debug:
                    traceback.print_exc()
            except:
                break
    time.sleep(10)  # wait for in-progress rsync job to complete
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
