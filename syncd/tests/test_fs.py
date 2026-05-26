import os
import socket
import pytest

from syncd.utils.fs import (
    ensure_dir,
    expand_path,
    is_dir,
    is_file,
    list_dir,
    read_file,
    remove_socket,
    write_file,
)


async def test_ensure_dir_creates_nested(tmp_path):
    target = str(tmp_path / "a" / "b" / "c")
    await ensure_dir(target)
    assert os.path.isdir(target)


async def test_ensure_dir_idempotent(tmp_path):
    target = str(tmp_path / "dir")
    await ensure_dir(target)
    await ensure_dir(target)
    assert os.path.isdir(target)


async def test_write_file_creates_and_reads_back(tmp_path):
    path = str(tmp_path / "out.bin")
    await write_file(path, b"hello world")
    result = await read_file(path)
    assert result == b"hello world"


async def test_write_file_atomic_no_tmp_left(tmp_path):
    path = str(tmp_path / "out.bin")
    await write_file(path, b"data")
    assert not os.path.exists(path + ".tmp")


async def test_write_file_overwrites(tmp_path):
    path = str(tmp_path / "f.bin")
    await write_file(path, b"first")
    await write_file(path, b"second")
    assert await read_file(path) == b"second"


async def test_is_dir_true(tmp_path):
    assert await is_dir(str(tmp_path)) is True


async def test_is_dir_false(tmp_path):
    assert await is_dir(str(tmp_path / "nope")) is False


async def test_is_file_true(tmp_path):
    p = tmp_path / "f.txt"
    p.write_bytes(b"x")
    assert await is_file(str(p)) is True


async def test_is_file_false(tmp_path):
    assert await is_file(str(tmp_path / "nope.txt")) is False


async def test_list_dir(tmp_path):
    (tmp_path / "a.txt").write_bytes(b"")
    (tmp_path / "b.txt").write_bytes(b"")
    names = await list_dir(str(tmp_path))
    assert set(names) == {"a.txt", "b.txt"}


async def test_remove_socket_removes(tmp_path):
    sock_path = str(tmp_path / "test.sock")
    s = socket.socket(socket.AF_UNIX)
    s.bind(sock_path)
    s.close()
    assert os.path.exists(sock_path)
    await remove_socket(sock_path)
    assert not os.path.exists(sock_path)


async def test_remove_socket_nonexistent_is_noop(tmp_path):
    await remove_socket(str(tmp_path / "missing.sock"))


async def test_remove_socket_does_not_remove_regular_file(tmp_path):
    p = tmp_path / "not_a_socket.txt"
    p.write_bytes(b"data")
    await remove_socket(str(p))
    assert p.exists()


def test_expand_path_tilde():
    result = expand_path("~/foo")
    assert not result.startswith("~")
    assert result == os.path.join(os.path.expanduser("~"), "foo")


def test_expand_path_env_var():
    os.environ["_SYNAK_TEST_DIR"] = "/tmp/testdir"
    result = expand_path("$_SYNAK_TEST_DIR/sub")
    assert result == "/tmp/testdir/sub"
