import asyncio
import os
import stat


def expand_path(path: str) -> str:
    return os.path.expandvars(os.path.expanduser(path))


async def ensure_dir(path: str) -> None:
    await asyncio.to_thread(os.makedirs, path, exist_ok=True)


async def remove_socket(path: str) -> None:
    def _remove() -> None:
        try:
            st = os.stat(path)
        except FileNotFoundError:
            return
        if stat.S_ISSOCK(st.st_mode):
            os.remove(path)

    await asyncio.to_thread(_remove)


async def is_dir(path: str) -> bool:
    return await asyncio.to_thread(os.path.isdir, path)


async def is_file(path: str) -> bool:
    return await asyncio.to_thread(os.path.isfile, path)


async def list_dir(path: str) -> list[str]:
    return await asyncio.to_thread(os.listdir, path)


async def read_file(path: str) -> bytes:
    def _read() -> bytes:
        with open(path, "rb") as f:
            return f.read()

    return await asyncio.to_thread(_read)


async def write_file(path: str, data: bytes) -> None:
    tmp = path + ".tmp"

    def _write() -> None:
        with open(tmp, "wb") as f:
            f.write(data)
        os.replace(tmp, path)

    await asyncio.to_thread(_write)
