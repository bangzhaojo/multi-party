import zstandard as zstd
import json
import os
from datetime import datetime
import logging.handlers
import re


log = logging.getLogger("bot")
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())

file_date = ['RC_2022-05', 'RC_2022-06', 'RC_2022-07']
file_root_rc = '/shared/2/datasets/reddit-dump-all/RC/'
output_root = '/home/bangzhao/projects/multi-party/data/RC/'


def read_and_decode(reader, chunk_size, max_window_size, previous_chunk=None, bytes_read=0):
    chunk = reader.read(chunk_size)
    bytes_read += chunk_size
    if previous_chunk is not None:
        chunk = previous_chunk + chunk
    try:
        return chunk.decode()
    except UnicodeDecodeError:
        if bytes_read > max_window_size:
            raise UnicodeError(f"Unable to decode frame after reading {bytes_read:,} bytes")
        print(f"Decoding error with {bytes_read:,} bytes, reading another chunk")
        return read_and_decode(reader, chunk_size, max_window_size, chunk, bytes_read)


def read_lines_zst(file_name):
    with open(file_name, 'rb') as file_handle:
        buffer = ''
        reader = zstd.ZstdDecompressor(max_window_size=2 ** 31).stream_reader(file_handle)
        while True:
            chunk = read_and_decode(reader, 2 ** 27, (2 ** 29) * 2)

            if not chunk:
                break
            lines = (buffer + chunk).split("\n")

            for line in lines[:-1]:
                yield line, file_handle.tell()

            buffer = lines[-1]

        reader.close()


if __name__ == "__main__":
    for date in file_date:

        file_path = file_root_rc + date + '.zst'
        output_path = output_root + 'filtered_' + date + '.json'

        file_size = os.stat(file_path).st_size
        file_lines = 0
        file_bytes_processed = 0
        created = None
        field = "body"
        values = [r'\brivals?\b', r'\brivalry\b', r'\brivalries\b', r'\brivalled\b', r'\brivaled\b', r'\brivaling\b']
        bad_lines = 0

        with open(output_path, 'w') as file:
            for line, file_bytes_processed in read_lines_zst(file_path):
                try:
                    obj = json.loads(line)
                    created = datetime.utcfromtimestamp(int(obj['created_utc']))

                    if any([re.search(value, obj[field]) for value in values]):

                        obj_new = dict()
                        obj_new['author'] = obj['author']
                        obj_new['author_fullname'] = obj['author_fullname']
                        obj_new['body'] = obj['body']
                        obj_new['controversiality'] = obj['controversiality']
                        obj_new['created_utc'] = obj['created_utc']
                        obj_new['id'] = obj['id']
                        obj_new['is_submitter'] = obj['is_submitter']
                        obj_new['link_id'] = obj['link_id']
                        obj_new['name'] = obj['name']
                        obj_new['parent_id'] = obj['parent_id']
                        obj_new['permalink'] = obj['permalink']
                        obj_new['score'] = obj['score']
                        obj_new['subreddit'] = obj['subreddit']
                        obj_new['subreddit_id'] = obj['subreddit_id']
                        obj_new['subreddit_name_prefixed'] = obj['subreddit_name_prefixed']
                        obj_new['subreddit_type'] = obj['subreddit_type']
                        obj_new['total_awards_received'] = obj['total_awards_received']

                        # print(json.dumps(obj_new, indent=4))  # show the formatted JSON data
                        file.write(json.dumps(obj_new))
                        file.write('\n')

                except (KeyError, json.JSONDecodeError) as err:
                    bad_lines += 1
                file_lines += 1

                if file_lines % 100000 == 0:
                    log.info(
                        f"{created.strftime('%Y-%m-%d %H:%M:%S')} : {file_lines:,} : {bad_lines:,} : {file_bytes_processed:,}:{(file_bytes_processed / file_size) * 100:.0f}%")
        file.close()
