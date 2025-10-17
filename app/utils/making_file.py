import os

import aiofiles


async def save_parsed_data(parsed_data, filename):
    results_dir = os.path.join(os.getcwd(), "results")
    os.makedirs(results_dir, exist_ok=True)
    file_path = os.path.join(results_dir, filename)

    async with aiofiles.open(file_path, "w", encoding="utf-8") as f:
        for block in parsed_data:
            await f.write("\n".join(block) + "\n\n")

    return file_path
