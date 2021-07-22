import gc
import hashlib
import multiprocessing as mp
import os
import random
import shutil
import time
import traceback
import warnings
from glob import glob
from io import BytesIO
from urllib.parse import urljoin, urlparse
from uuid import uuid1

import asks
import ftfy
import pandas as pd
import pycld2 as cld2
import tractor
import trio
import ujson
from bloom_filter2 import BloomFilter
from PIL import Image, ImageFile, UnidentifiedImageError

asks.init('trio')

ImageFile.LOAD_TRUNCATED_IMAGES = True  # https://stackoverflow.com/a/47958486


warnings.filterwarnings("ignore")


def chunk_using_generators(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i: i + n]


def remove_bad_chars(text):
    return "".join(c for c in text if c.isprintable())


def parse_wat(content, start, line_count, blocked, bloom_filter):
    dedupes = 0
    valid_data = []
    content.seek(start)
    for _ in range(line_count):
        line = content.readline()

        if "IMG@" not in line:
            continue

        line_str = line.strip()
        data = ujson.loads(line_str)

        linklist = data["Envelope"]["Payload-Metadata"]["HTTP-Response-Metadata"][
            "HTML-Metadata"
        ]["Links"]

        base_url = os.path.dirname(
            data["Envelope"]["WARC-Header-Metadata"]["WARC-Target-URI"]
        )  # get base url

        license = "?"
        for e in linklist:
            if "url" in e and "creativecommons.org/licenses/" in e["url"]:
                license = e["url"]
            if "alt" not in e:
                continue
            url = e["url"]

            if any(x in url for x in [".svg", ".gif", "data:image", "javascript:"]):
                continue

            try:
                if urlparse(url).netloc in blocked:
                    continue
            except:
                continue

            alt_text = ftfy.fix_text(e["alt"].replace("\n", " ")).strip()
            try:
                _, _, details = cld2.detect(alt_text)
            except Exception as e:
                alt_text = remove_bad_chars(alt_text)
                _, _, details = cld2.detect(alt_text)

            if details[0][1] == "en":
                if not url.startswith("http"):
                    url = urljoin(base_url, url)

                if hashlib.md5((url + alt_text).encode("utf-8")).hexdigest() in bloom_filter:
                    dedupes += 1
                    continue

                valid_data.append((url, alt_text, license))
    return [
        t for t in {tuple(i) for i in valid_data}
    ], dedupes  # Remove duplicate tuple from list


def process_img_content(response, alt_text, license, sample_id):
    img_output_folder = "save/images/"

    try:
        if len(response.content) < 5000:
            return
        img_data = BytesIO(response.content)
        with Image.open(img_data) as im:
            width, height = im.size
            im_format = im.format
            out_fname = f"{img_output_folder}{str(sample_id)}.{im_format.lower()}"
            if im_format not in ["JPEG", "JPG", "PNG"]:
                return
            if im.mode != "RGB":
                im = im.convert("RGB")
            im.save(out_fname)
    except (KeyError, UnidentifiedImageError):
        return

    return [str(sample_id), out_fname, response.url, alt_text, width, height, license]


async def request_image(datas, start_sampleid):
    tmp_data = []
    session = asks.Session(connections=165)
    session.headers = {
        "User-Agent": "Crawling at Home Project (http://cah.io.community)",
        "Accept-Language": "en-US",
        "Accept-Encoding": "gzip, deflate",
        "Referer": "https://commoncrawl.org",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }

    async def _request(data, sample_id):
        url, alt_text, license = data
        try:
            proces = process_img_content(
                await session.get(url, timeout=5), alt_text, license, sample_id
            )
            if proces is not None:
                tmp_data.append(proces)
        except Exception:
            return

    async with trio.open_nursery() as n:
        for data in datas:
            n.start_soon(_request, data, start_sampleid)
            start_sampleid += 1

    with open(f".tmp/{uuid1()}.json", "w") as f:
        ujson.dump(tmp_data, f)
    gc.collect()
    return


def dl_wat(valid_data, first_sample_id):
    # Download every image available
    processed_samples = []
    n_processes = mp.cpu_count()

    if n_processes == 1:
        trio.run(request_image, valid_data, first_sample_id)
    else:
        async def _runtractor():
            async with tractor.open_nursery() as n:
                chunk_size = len(valid_data) // n_processes + 1
                for i, data in enumerate(chunk_using_generators(valid_data, chunk_size)):
                    await n.run_in_actor(
                        request_image, datas=data, start_sampleid=first_sample_id + i * chunk_size
                    )

        trio.run(_runtractor)

    for tmpf in glob(".tmp/*.json"):
        processed_samples.extend(ujson.load(open(tmpf)))
    return pd.DataFrame(
        processed_samples,
        columns=["SAMPLE_ID", "PATH", "URL",
                 "TEXT", "HEIGHT", "WIDTH", "LICENSE"],
    )


def upload(source: str, client_type: str):
    client_type = client_type.upper()
    target = 'gpujobs' if client_type == 'CPU' else 'CAH'
    options = '-rsh' if client_type == 'CPU' else '-zh'
    return os.system(f'rsync {options} {source} archiveteam@88.198.2.17::{target}')


iters = 0


def updateFilters(bloom=None, blocked=None):
    if iters//10:
        return bloom, blocked

    shutil.rmtree('blocklists', ignore_errors=True)

    result = 1
    while result:
        result = os.system(
            "rsync -zh archiveteam@88.198.2.17::bloom/*.bin blocklists")

    bloom = BloomFilter(max_elements=80_000_000,
                        error_rate=0.01, filename=("blocklists/bloom.bin", -1))

    blocked = BloomFilter(max_elements=10_000_000, error_rate=0.01, filename=(
        "blocklists/failed-domains.bin", -1))

    return bloom, blocked


class FileData:
    def __init__(self, filename):
        self._filename = filename
        self._line_to_position = [0]
        self._length = 0

        with open(self._filename, "r") as f:
            while f.readline():
                self._line_to_position.append(f.tell())
                self._length += 1

    def __getitem__(self, line):
        return self._line_to_position[line]

    def __len__(self):
        return self._length


def main(name, url, debug):
    global iters

    import crawlingathome_client as cah

    print('[crawling@home] loading clip')
    from clip_filter import run_inference
    print('\n[crawling@home] clip loaded\n')

    bloom_filter, blocked_links = updateFilters()

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

            bloom_filter, blocked_links = updateFilters(
                bloom=bloom_filter, blocked=blocked_links)

            start = time.time()

            shutil.rmtree(output_folder, ignore_errors=True)
            shutil.rmtree(img_output_folder, ignore_errors=True)
            shutil.rmtree(".tmp", ignore_errors=True)

            os.mkdir(output_folder)
            os.mkdir(img_output_folder)
            os.mkdir(".tmp")

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
            client.log("Processing shard")
            start_processing = time.time()

            fd = FileData("shard.wat")

            if shard_of_chunk == 0:
                start_index = fd[0]
            if shard_of_chunk == 1:
                start_index = fd[int(len(fd) * 0.5)]

            lines = int(len(fd) * 0.5)

            with open("shard.wat", "r") as infile:
                parsed_data, dedupes = parse_wat(
                    infile, start_index, lines, blocked_links, bloom_filter)

            parsed_df = pd.DataFrame(parsed_data, columns=[
                                     "URL", "TEXT", "LICENSE"])

            num_links = len(parsed_df)
            del parsed_df

            random.shuffle(parsed_data)

            end_processing = time.time()
            print(
                f'[crawling@home] processed shard in {end_processing - start_processing}, duplicates found: {dedupes}')

            client.log("Downloading images")
            start_dl = time.time()
            dlparse_df = dl_wat(parsed_data, first_sample_id)
            dlparse_df.to_csv(
                f'{output_folder}{out_fname}.csv', index=False, sep="|")
            end_dl = time.time()
            print(
                f"[crawling@home] Downloaded {len(dlparse_df)} images out of {num_links} links in {(end_dl - start_dl):.1f} seconds")
            print(
                f"[crawling@home] Download efficiency: {(len(dlparse_df) / (end_dl - start_dl)):.2f} img/sec OR {(num_links / (end_dl - start_dl)):.2f} links/sec")

            client.log("Dropping NSFW keywords")

            filtered_df_len = run_inference(
                dlparse_df, output_folder, out_fname)

            client.log("Uploading Results")

            upload(f'{output_folder}/*{out_fname}*', client.type)

            client.completeJob(filtered_df_len)
            end = time.time()
            print(
                f"[crawling@home] job completed in {(end - start):.1f} seconds")
            print(
                f"[crawling@home] job efficiency {(filtered_df_len / (end - start)):.2f} pairs/sec")

            iters += 1

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
