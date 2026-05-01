"""
Parallel SSH command dispatch to multiple POWDER nodes.
Requires: asyncssh, ~/.ssh/config with POWDER host aliases.
"""
import asyncio
import asyncssh
from typing import Sequence


async def _run_one(host: str, cmd: str, username: str = "zifanzhang") -> tuple[str, str, str]:
    async with asyncssh.connect(host, username=username, known_hosts=None) as conn:
        result = await conn.run(cmd, check=False)
        return host, result.stdout, result.stderr


async def fanout(hosts: Sequence[str], cmd: str, username: str = "zifanzhang") -> dict[str, tuple[str, str]]:
    tasks = [_run_one(h, cmd, username) for h in hosts]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    out = {}
    for r in results:
        if isinstance(r, Exception):
            print(f"[ERROR] {r}")
        else:
            host, stdout, stderr = r
            out[host] = (stdout, stderr)
            if stderr:
                print(f"[{host}] STDERR: {stderr.strip()}")
    return out


def run(hosts: Sequence[str], cmd: str, username: str = "zifanzhang") -> dict[str, tuple[str, str]]:
    return asyncio.run(fanout(hosts, cmd, username))


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 3:
        print("Usage: ssh_fanout.py 'host1,host2' 'command'")
        sys.exit(1)
    hosts = sys.argv[1].split(",")
    cmd = sys.argv[2]
    results = run(hosts, cmd)
    for host, (stdout, _) in results.items():
        print(f"\n=== {host} ===\n{stdout}")
