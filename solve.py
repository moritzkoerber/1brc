import mmap
import os
import sys
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from gc import disable, enable

OUTPUT_FILE = "output.txt"


def calculate_chunk_offsets(path: str, n_chunks: int) -> list[tuple[int, int]]:
    offsets = []
    file_size = os.path.getsize(path)
    chunk_size = file_size // n_chunks
    chunk_start = 0

    with open(path, "rb") as f:
        for _ in range(n_chunks):
            if (end := chunk_start + chunk_size) > file_size:
                offsets.append((chunk_start, file_size))
                break
            else:
                f.seek(end)
                if line := f.readline():
                    end += len(line)
                offsets.append((chunk_start, end))
                chunk_start = end
    return offsets


def process_chunk(path: str, offset: tuple[int, int]) -> dict[bytes, list[int | float]]:
    stations = defaultdict(lambda: [float("-inf"), float("inf"), 0.0, 0])
    start, end = offset

    with open(path, "rb") as f:
        mmapped_file = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        mmapped_file.seek(start)
        disable()
        while mmapped_file.tell() < end:
            k, v = mmapped_file.readline().split(b";")
            v = float(v)
            station = stations[k]
            if v > station[0]:
                station[0] = v
            if v < station[1]:
                station[1] = v
            station[2] += v
            station[3] += 1
        enable()
    return dict(stations)


def combine_results(
    stations: dict[bytes, list[int | float]],
    combined_result: dict[bytes, list[int | float]],
) -> dict[bytes, list]:
    for k, (v_max, v_min, v_sum, v_count) in stations.items():
        station = combined_result[k]
        if v_max > station[0]:
            station[0] = v_max
        if v_min < station[1]:
            station[1] = v_min
        station[2] += v_sum
        station[3] += v_count
    return combined_result


def main():
    try:
        _, path, cores = sys.argv
    except ValueError:
        print("Usage: python solve.py <path-to-measurements> <number-of-cores>")
        sys.exit(1)

    chunk_offsets = calculate_chunk_offsets(path, int(cores))

    with ProcessPoolExecutor(max_workers=int(cores)) as executor:
        results = [
            executor.submit(process_chunk, path, chunk_offset)
            for chunk_offset in chunk_offsets
        ]

    combined_result = defaultdict(lambda: [float("-inf"), float("inf"), 0.0, 0])

    for executor_result in as_completed(results):
        combined_result = combine_results(executor_result.result(), combined_result)

    print(
        f"{{{
            ', '.join(
                (
                    f'{k.decode("utf-8")}={v_min}/{v_sum / v_count:.1f}/{v_max}'
                    for k, (v_max, v_min, v_sum, v_count) in sorted(
                        combined_result.items()
                    )
                )
            )
        }}}\n"
    )


if __name__ == "__main__":
    main()
