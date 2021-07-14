import gc
import os
import random
import shutil
import time
from glob import glob
from io import BytesIO
import traceback
from urllib.parse import urljoin, urlparse
from uuid import uuid1

import tractor
import trio
import ujson
from PIL import Image, ImageFile, UnidentifiedImageError

ImageFile.LOAD_TRUNCATED_IMAGES = True  # https://stackoverflow.com/a/47958486

import warnings
warnings.filterwarnings("ignore")

def upload(source: str, client_type: str):
    client_type = client_type.upper()
    target = 'gpujobs' if client_type == 'CPU' else 'CAH'
    options = '-rsh' if client_type == 'CPU' else '-zh'
    return os.system(f'rsync {options} {source} archiveteam@88.198.2.17::{target}')

def main(name, url, debug):
    import crawlingathome_client as cah

    print('[crawling@home] loading clip')
    from clip_filter import run_inference
    print('\n[crawling@home] clip loaded\n')

    client = cah.init(
        url=url, nickname=name
    )

    output_folder = "./save/"
    img_output_folder = output_folder + "images/"

    while client.jobCount() > 0:
        try:
            if not client.isAlive():
                client = cah.init(
                    url=url, nickname=name
                )

            start = time.time()

            if os.path.exists(output_folder):
                shutil.rmtree(output_folder)

            os.mkdir(output_folder)
            os.mkdir(img_output_folder)

            client.newJob()
            client.downloadShard()

            first_sample_id = int(client.start_id)
            last_sample_id = int(client.end_id)
            shard_of_chunk = client.shard_piece

            out_fname = \
                f"FIRST_SAMPLE_ID_IN_SHARD_{first_sample_id}_LAST_SAMPLE_ID_IN_SHARD_{last_sample_id}_{shard_of_chunk}"
            print(
                f"[crawling@home] shard identification {out_fname}"
            )  # in case test fails, we need to remove bad data
            
            dlparse_df = None
            print(f"[crawling@home] Downloaded {len(dlparse_df)} in {round(time.time() - start)} seconds")
            print(f"[crawling@home] Download efficiency {len(dlparse_df) / (time.time() - start)} img/sec")

            client.log("Dropping NSFW keywords")

            filtered_df_len = run_inference(dlparse_df, output_folder, out_fname)

            client.log("Uploading Results")

            upload(f'{output_folder}/*{out_fname}*', client.type)

            client.completeJob(filtered_df_len)
            end = time.time()
            print(f"[crawling@home] job completed in {round(end - start)} seconds")
            print(f"[crawling@home] job efficiency {filtered_df_len / (end - start)} pairs/sec")

            if debug:
                break
        except KeyboardInterrupt:
            print("[crawling@home] stopping crawler")
            break
        except Exception as ex:
            print(f"[crawling@home] ERROR: {ex}")
            if debug:
                traceback.print_exc()
                break
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
