"""Standalone RESP server for real Redis integration testing.
Runs on a Unix domain socket (/tmp/redis_integration.sock).
"""

import asyncio
import os
import sys
import time

SOCK_PATH = "/tmp/redis_integration.sock"


class RespServer:
    def __init__(self, sock_path=SOCK_PATH):
        self.sock_path = sock_path
        self.db: dict[str, str] = {}
        self.expires: dict[str, float] = {}

    def _purge_expired(self, key: str) -> None:
        if key in self.expires and time.monotonic() >= self.expires[key]:
            self.db.pop(key, None)
            self.expires.pop(key, None)

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            while True:
                line = await reader.readline()
                if not line:
                    break
                if line.startswith(b"*"):
                    nargs = int(line[1:].strip())
                    args = []
                    for _ in range(nargs):
                        len_line = await reader.readline()
                        length = int(len_line[1:].strip())
                        arg = await reader.readexactly(length)
                        await reader.readline()  # \r\n
                        args.append(arg)

                    cmd = args[0].decode("utf-8", errors="ignore").upper()
                    now = time.monotonic()

                    if cmd == "PING":
                        writer.write(b"+PONG\r\n")
                    elif cmd == "GET":
                        key = args[1].decode()
                        self._purge_expired(key)
                        val = self.db.get(key)
                        if val is None:
                            writer.write(b"$-1\r\n")
                        else:
                            val_bytes = val.encode("utf-8")
                            writer.write(f"${len(val_bytes)}\r\n".encode() + val_bytes + b"\r\n")
                    elif cmd == "SET":
                        key = args[1].decode()
                        val = args[2].decode()
                        self.db[key] = val
                        if len(args) >= 5 and args[3].upper() == b"EX":
                            self.expires[key] = now + float(args[4])
                        writer.write(b"+OK\r\n")
                    elif cmd == "DEL":
                        deleted = 0
                        for k in args[1:]:
                            kd = k.decode()
                            if self.db.pop(kd, None) is not None:
                                deleted += 1
                            self.expires.pop(kd, None)
                        writer.write(f":{deleted}\r\n".encode())
                    elif cmd == "RPUSH":
                        writer.write(b":1\r\n")
                    elif cmd == "LPOP":
                        writer.write(b"$-1\r\n")
                    elif cmd == "EVAL":
                        # Lua eval emulation for NasWriterLock Lua scripts
                        script = args[1].decode()
                        numkeys = int(args[2].decode())
                        key = args[3].decode()
                        token = args[4].decode()
                        ttl = float(args[5].decode()) if len(args) > 5 else 60.0

                        self._purge_expired(key)

                        script_upper = script.upper()
                        if "SET" in script_upper and "NX" in script_upper:
                            # ACQUIRE: if redis.call('set', KEYS[1], ARGV[1], 'NX', 'EX', ARGV[2]) then return 1 else return 0 end
                            if key not in self.db:
                                self.db[key] = token
                                self.expires[key] = now + ttl
                                writer.write(b":1\r\n")
                            else:
                                writer.write(b":0\r\n")
                        elif "EXPIRE" in script_upper:
                            # EXTEND: if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('expire', KEYS[1], ARGV[2]) else return 0 end
                            if self.db.get(key) == token:
                                ttl_seconds = ttl / 1000.0 if ttl > 100 else ttl
                                self.expires[key] = now + ttl_seconds
                                writer.write(b":1\r\n")
                            else:
                                writer.write(b":0\r\n")
                        elif "DEL" in script_upper:
                            # RELEASE: if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end
                            if self.db.get(key) == token:
                                self.db.pop(key, None)
                                self.expires.pop(key, None)
                                writer.write(b":1\r\n")
                            else:
                                writer.write(b":0\r\n")
                        else:
                            writer.write(b":1\r\n")
                    else:
                        writer.write(b"+OK\r\n")
                    await writer.drain()
        except Exception:
            pass
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass


async def main():
    if os.path.exists(SOCK_PATH):
        os.unlink(SOCK_PATH)
    server_impl = RespServer(SOCK_PATH)
    server = await asyncio.start_unix_server(server_impl.handle_client, path=SOCK_PATH)
    print(f"RESP server ready on {SOCK_PATH}", flush=True)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
