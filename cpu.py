import gc
import hashlib
import multiprocessing as mp
import os
import random
import shutil
import time
import traceback
import warnings
from ctypes import c_bool, c_int
from functools import partial
from glob import glob
from io import BytesIO
from threading import Thread
from urllib.parse import urljoin, urlparse
from uuid import uuid1, uuid4

import asks
import ftfy
import pandas as pd
import pycld2 as cld2
import requests
import trio
import ujson
from bloom_filter2 import BloomFilter
from PIL import Image, ImageFile, UnidentifiedImageError
from requests.adapters import HTTPAdapter

ImageFile.LOAD_TRUNCATED_IMAGES = True  # https://stackoverflow.com/a/47958486

warnings.filterwarnings('ignore')


def chunk_using_generators(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i: i + n]


def remove_bad_chars(text):
    return ''.join(c for c in text if c.isprintable())


def parse_wat_worker(file_name, start, line_count, oneprocess=False):
    bloom_filter, blocked_links, clipped_filter = getFilters()
    blocked_formats = set(
        ['.svg', '.gif', '.webp', 'data:image', 'javascript:', 'mailto:'])

    dedupes = 0
    cliped = 0
    valid_data = []
    with open(file_name, 'r') as content:
        content.seek(start)
        for _ in range(line_count):
            line = content.readline()

            if 'IMG@' not in line:
                continue

            line_str = line.strip()
            data = ujson.loads(line_str)

            linklist = data['Envelope']['Payload-Metadata']['HTTP-Response-Metadata'][
                'HTML-Metadata'
            ]['Links']

            base_url = os.path.dirname(
                data['Envelope']['WARC-Header-Metadata']['WARC-Target-URI']
            )  # get base url

            license = '?'
            for e in linklist:
                if 'url' in e and 'creativecommons.org/licenses/' in e['url']:
                    license = e['url']
                if 'alt' not in e:
                    continue
                url = e['url']

                try:
                    if urlparse(url).netloc in blocked_links:
                        continue
                except:
                    continue

                alt_text = ftfy.fix_text(e['alt'].replace('\n', ' ')).strip()
                try:
                    _, _, details = cld2.detect(alt_text)
                except Exception as e:
                    alt_text = remove_bad_chars(alt_text)
                    _, _, details = cld2.detect(alt_text)

                if details[0][1] == 'en':
                    if not url.startswith('http'):
                        url = urljoin(base_url, url)
                    dedupe_url = hashlib.md5(
                        (url + alt_text).encode('utf-8')).hexdigest()
                    if any(bf in url for bf in blocked_formats):
                        continue
                    if dedupe_url in bloom_filter:
                        dedupes += 1
                        continue
                    elif dedupe_url in clipped_filter:
                        cliped += 1
                        continue

                    valid_data.append((url, alt_text, license))
        if oneprocess:
            orig_len = len(valid_data)
            data = [
                t for t in {tuple(i) for i in valid_data}
            ]
            shard_dups = orig_len - len(data)
            return data, dedupes, cliped, shard_dups

        with open(f'.tmp/pw-{uuid1()}.json', 'w') as f:
            ujson.dump(valid_data + [dedupes, cliped], f)


def parse_wat(file_name, shard, workers):
    fd = FileData(file_name)

    if shard == 0:
        start_line = 0
    elif shard == 1:
        start_line = len(fd)//2

    line_count = len(fd)//2

    if workers == 1:
        return parse_wat_worker(file_name, fd[start_line], line_count, oneprocess=True)

    lc = line_count//workers - 1
    with mp.Pool(workers) as pool:
        pool.starmap(parse_wat_worker, [
                     (file_name, fd[start_line + i*lc], lc) for i in range(workers)])

    valid_data = []
    dedupes = 0
    cliped = 0
    for tmpf in glob('.tmp/pw-*.json'):
        with open(tmpf, 'r') as f:
            tmp_data = ujson.load(f)
            valid_data.extend(tmp_data[:-2])
            dedupes += tmp_data[-2]
            cliped += tmp_data[-1]
    orig_len = len(valid_data)
    data = [
        t for t in {tuple(i) for i in valid_data}
    ]
    shard_dups = orig_len - len(data)
    del fd
    return data, dedupes, cliped, shard_dups


def process_img_content(response, alt_text, license, sample_id):
    img_output_folder = 'save/images/'

    try:
        if len(response.content) < 5000:
            return
        img_data = BytesIO(response.content)
        with Image.open(img_data) as im:
            width, height = im.size
            im_format = im.format
            out_fname = f'{img_output_folder}{str(sample_id)}.{im_format.lower()}'
            if im_format not in ['JPEG', 'JPG', 'PNG']:
                return
            if im.mode != 'RGB':
                im = im.convert('RGB')
            im.save(out_fname)
    except (KeyError, UnidentifiedImageError):
        return

    return [str(sample_id), out_fname, response.url, alt_text, width, height, license]


async def request_image(datas, start_sampleid, processing_count, lock):
    limit = trio.CapacityLimiter(165*2)
    tmp_data = []
    session = asks.Session(connections=165)
    session.headers = {
        'User-Agent': 'Crawling at Home Project (http://cah.io.community)',
        'Accept-Language': 'en-US',
        'Accept-Encoding': 'gzip, deflate',
        'Referer': 'https://www.google.com',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    }

    async def _request(data, sample_id):
        task = trio.lowlevel.current_task()
        with lock:
            processing_count.value += 1

        url, alt_text, license = data
        try:
            proces = process_img_content(
                await session.get(url, timeout=5), alt_text, license, sample_id
            )
            task.custom_sleep_data = 0
            if proces is not None:
                tmp_data.append(proces)
        except Exception:
            task.custom_sleep_data = 1

    async with trio.open_nursery() as n:
        for data in datas:
            async with limit:
                n.start_soon(_request, data, start_sampleid)
            start_sampleid += 1

    with open(f'.tmp/dl-{uuid1()}.json', 'w') as f:
        ujson.dump(tmp_data, f)
    gc.collect()


def dl_wat_worker(data, start_sample_id, processing_count, finished_count, error_count, lock):
    trio.run(request_image, data, start_sample_id, processing_count, lock, instruments=[
             DownloadProgressInstrument(processing_count, finished_count, error_count, lock)])


def dl_progress(len_data, processing_count, finished_count, update_tqdm, lock, isnotebook=False):
    if isnotebook:
        from tqdm import tqdm
    else:
        from tqdm import tqdm

    progress_bar = tqdm(total=len_data, unit='links')
    while True:
        with lock:
            if not update_tqdm.value:
                break
            progress_bar.desc = f'Processing {processing_count.value} links'
            progress_bar.update(finished_count.value - progress_bar.n)
        time.sleep(1)
    progress_bar.close()


def dl_wat(valid_data, first_sample_id, isnotebook=False):
    # Download every image available
    processed_samples = []
    n_processes = mp.cpu_count()

    manager = mp.Manager()
    update_tqdm = manager.Value(c_bool, True)

    processing_count = manager.Value(c_int, 0)
    finished_count = manager.Value(c_int, 0)
    lock = manager.Lock()

    t = mp.Process(target=dl_progress, args=(
        len(valid_data), processing_count, finished_count, update_tqdm, lock, isnotebook))
    t.start()

    if n_processes == 1:
        dl_wat_worker(valid_data, processing_count,
                      finished_count, update_tqdm, lock, isnotebook)
    else:
        chunk_size = len(valid_data) // n_processes + 1
        worker = partial(dl_wat_worker, processing_count=processing_count,
                         finished_count=finished_count, lock=lock)
        with mp.Pool(n_processes) as pool:
            pool.starmap(worker, [(data, first_sample_id + i * chunk_size)
                                  for (i, data) in enumerate(chunk_using_generators(valid_data, chunk_size))])

    time.sleep(1)
    with lock:
        update_tqdm.value = False

    t.join()
    t.close()

    for tmpf in glob('.tmp/dl-*.json'):
        with open(tmpf, 'r') as f:
            processed_samples.extend(ujson.load(f))
    return pd.DataFrame(
        processed_samples,
        columns=['SAMPLE_ID', 'PATH', 'URL',
                 'TEXT', 'HEIGHT', 'WIDTH', 'LICENSE'],
    )


def upload(source: str, client_type: str):
    client_type = client_type.upper()
    target = 'gpujobs' if client_type == 'CPU' else 'CAH'
    options = '-rsh' if client_type == 'CPU' else '-zh'
    return os.system(f'rsync {options} {source} archiveteam@88.198.2.17::{target}')


def updateFilters():
    start = time.time()
    shutil.rmtree('blocklists', ignore_errors=True)
    os.mkdir('blocklists')

    session = requests.Session()
    session.headers.update({'User-Agent': 'Crawling@Home'})
    session.mount('http://', HTTPAdapter(max_retries=15))

    url = 'http://the-eye.eu/public/AI/cahblacklists/{}'

    def _updateFilter(blocklist):
        with session.get(url.format(blocklist), stream=True) as r:
            r.raise_for_status()
            with open(f'blocklists/{blocklist}', 'w+b') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)

    processes = []
    for blocklist in ('bloom200M.bin', 'clipped.bin', 'failed-domains.bin'):
        p = Thread(target=_updateFilter, args=(blocklist,))
        p.start()
        processes.append(p)

    for p in processes:
        p.join()

    end = time.time()
    print(f'[crawling@home] updated filters in {(end-start):.1f}')


def getFilters():
    bloom = BloomFilter(max_elements=200_000_000,
                        error_rate=0.05, filename=('blocklists/bloom200M.bin', -1))

    blocked = BloomFilter(max_elements=10_000_000, error_rate=0.01, filename=(
        'blocklists/failed-domains.bin', -1))

    clipped = BloomFilter(max_elements=200_000_000, error_rate=0.05,
                          filename=('blocklists/clipped.bin', -1))

    return bloom, blocked, clipped


class DownloadProgressInstrument(trio.abc.Instrument):
    def __init__(self, processing_count, finished_count, lock):
        self._processing_count = processing_count
        self._finished_count = finished_count
        self._lock = lock

    def task_exited(self, task):
        if task.custom_sleep_data in [0, 1]:
            with self._lock:
                self._finished_count.value += 1


class FileData:
    def __init__(self, filename):
        self._filename = filename
        self._line_to_position = [0]
        self._length = 0

        with open(self._filename, 'r') as f:
            while f.readline():
                self._line_to_position.append(f.tell())
                self._length += 1
        gc.collect()

    def __getitem__(self, line):
        return self._line_to_position[line]

    def __len__(self):
        return self._length


def main(name, url, debug, isnotebook):
    import crawlingathome_client as cah

    client = cah.init(
        url=url, nickname=name, type='cpu'
    )

    if not os.path.exists('blocklists'):
        os.mkdir('blocklists')

    output_folder = './save/'
    img_output_folder = output_folder + 'images/'

    uid = ''
    updater = None
    workers = mp.cpu_count()
    while True:
        try:
            jobs = client.jobCount()
        except:
            pass
        break
    while jobs > 0:
        try:
            start = time.time()

            if workers == 1:
                updater = Thread(target=updateFilters)
            else:
                updater = mp.Process(target=updateFilters)
            updater.start()

            if not client.isAlive():
                client.recreate()

            shutil.rmtree(output_folder, ignore_errors=True)
            shutil.rmtree(uid, ignore_errors=True)
            shutil.rmtree('.tmp', ignore_errors=True)

            os.mkdir(output_folder)
            os.mkdir(img_output_folder)
            os.mkdir('.tmp')

            client.newJob()
            client.downloadShard()

            first_sample_id = int(client.start_id)
            last_sample_id = int(client.end_id)
            shard_of_chunk = client.shard_piece

            out_fname = \
                f'FIRST_SAMPLE_ID_IN_SHARD_{first_sample_id}_LAST_SAMPLE_ID_IN_SHARD_{last_sample_id}_{shard_of_chunk}'
            print(
                f'[crawling@home] shard identification {out_fname}'
            )  # in case test fails, we need to remove bad data

            updater.join()
            if hasattr(updater, 'close'):
                updater.close()

            client.log('Processing shard')
            start_processing = time.time()

            parsed_data, dedupes, cliped, shard_dups = parse_wat(
                'shard.wat', shard_of_chunk, workers)

            num_links = len(parsed_data)

            random.shuffle(parsed_data)

            end_processing = time.time()
            print(
                f'[crawling@home] Processed shard in {(end_processing-start_processing):.1f} seconds',
                f'duplicates found: {dedupes}, cliped found: {cliped}, shard dups found: {shard_dups}', sep='\n\t')

            client.log('Downloading images')
            start_dl = time.time()
            dlparse_df = dl_wat(parsed_data, first_sample_id, isnotebook)
            dlparse_df.to_csv(
                f'{output_folder}{out_fname}.csv', index=False, sep='|')
            end_dl = time.time()

            print(
                f'[crawling@home] Downloaded {len(dlparse_df)} images out of {num_links} links in {(end_dl - start_dl):.1f} seconds')
            print(
                f'[crawling@home] Download efficiency: {(len(dlparse_df) / (end_dl - start_dl)):.2f} img/sec OR {(num_links / (end_dl - start_dl)):.2f} links/sec')

            client.log('Uploading Temporary Job')

            uid = uuid4().hex
            shutil.copytree('save', uid)

            result = 1
            while result:
                result = upload(uid, client.type)

            client.completeJob(f'rsync {uid}')

            end = time.time()
            print(
                f'[crawling@home] job completed in {(end - start):.1f} seconds')
            print(
                f'[crawling@home] job efficiency {(len(dlparse_df) / (end - start)):.2f} pairs/sec')
        except KeyboardInterrupt:
            print('[crawling@home] stopping crawler')
            break
        except Exception as ex:
            print(f'[crawling@home] ERROR: {ex}')
            if debug:
                traceback.print_exc()
            try:
                if client.isAlive():
                    client.log('Error, restarting job')
            except:
                print("[crawling@home] Couldn't log to client:")
        finally:
            if debug:
                break
    try:
        if updater is not None:
            updater.join()
            if hasattr(updater, 'close'):
                updater.close()
    except:
        pass
    try:
        if client.isAlive():
            client.bye()
    except:
        pass
