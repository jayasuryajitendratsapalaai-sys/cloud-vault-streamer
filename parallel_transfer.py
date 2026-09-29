import math
import os
import re
import asyncio
import io
import time
from telethon.tl.functions.upload import GetFileRequest, SaveBigFilePartRequest, SaveFilePartRequest
from telethon.tl.types import InputFile, InputFileBig

from telethon.errors import FloodWaitError, ServerError, RpcCallFailError

async def fast_download_file(client, location, file_size, workers=2, part_size_kb=512, max_retries=8):
    part_size = part_size_kb * 1024
    part_count = (file_size + part_size - 1) // part_size
    buffer = bytearray(file_size)

    sem = asyncio.Semaphore(workers)
    flood_lock = asyncio.Lock()

    async def download_part(part_index):
        offset = part_index * part_size
        req = GetFileRequest(location, offset=offset, limit=part_size)

        for attempt in range(max_retries):
            try:
                async with sem:
                    # Stagger requests to avoid flood triggers
                    await asyncio.sleep(0.15 * part_index % workers)
                    result = await client(req)
                    buffer[offset:offset + len(result.bytes)] = result.bytes
                    return
            except FloodWaitError as fwe:
                async with flood_lock:
                    wait_time = fwe.seconds + 3
                    print(f"⏳ FloodWait on part {part_index}: sleeping {wait_time}s...")
                    await asyncio.sleep(wait_time)
            except Exception as e:
                err_str = str(e)
                m_wait = re.search(r"FLOOD_(?:PREMIUM_)?WAIT_(\d+)", err_str)
                if m_wait:
                    async with flood_lock:
                        wait_sec = int(m_wait.group(1)) + 3
                        print(f"⏳ Premium FloodWait on part {part_index}: sleeping {wait_sec}s...")
                        await asyncio.sleep(wait_sec)
                    continue
                if attempt == max_retries - 1:
                    raise e
                sleep_time = min(15, (2 ** attempt) + 1)
                await asyncio.sleep(sleep_time)

    # Process parts in small sequential batches to avoid flood
    batch_size = max(2, workers)
    for batch_start in range(0, part_count, batch_size):
        batch_end = min(batch_start + batch_size, part_count)
        tasks = [download_part(i) for i in range(batch_start, batch_end)]
        await asyncio.gather(*tasks)
        if batch_end < part_count:
            await asyncio.sleep(0.2)  # Brief pause between download batches
    return io.BytesIO(buffer)

async def fast_upload_file(client, file_data, file_name, workers=4, part_size_kb=512, max_retries=2):
    file_size = len(file_data)
    part_size = part_size_kb * 1024
    part_count = (file_size + part_size - 1) // part_size
    import telethon.helpers
    is_big = file_size > 10 * 1024 * 1024
    file_id = telethon.helpers.generate_random_long()

    sem = asyncio.Semaphore(workers)

    async def upload_part(part_index):
        offset = part_index * part_size
        chunk = file_data[offset:offset + part_size]
        if is_big:
            req = SaveBigFilePartRequest(file_id, part_index, part_count, chunk)
        else:
            req = SaveFilePartRequest(file_id, part_index, chunk)

        for attempt in range(max_retries):
            try:
                async with sem:
                    await client(req)
                    return
            except FloodWaitError as fwe:
                await asyncio.sleep(fwe.seconds + 2)
            except (ServerError, RpcCallFailError, ConnectionError, asyncio.TimeoutError, Exception) as e:
                if attempt == max_retries - 1:
                    raise e
                sleep_time = min(30, (2 ** attempt) + 1)
                await asyncio.sleep(sleep_time)

    tasks = [upload_part(i) for i in range(part_count)]
    await asyncio.gather(*tasks)

    if is_big:
        return InputFileBig(file_id, part_count, file_name)
    else:
        return InputFile(file_id, part_count, file_name, "")

