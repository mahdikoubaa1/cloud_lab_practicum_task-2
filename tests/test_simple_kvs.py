#!/usr/bin/env python3

import sys, random
from time import sleep
from testsupport import subtest
from socketsupport import run_router, run_kvs, run_ctl

def main() -> None:
    with subtest("Testing simple kvs with one shard"):
        router = run_router("127.0.0.1:40000", "127.0.0.1:41000")
        kvs    = run_kvs("127.0.0.1:42000", "127.0.0.1:43000", "127.0.0.1:41000")

        sleep(5)

        ctl    = run_ctl("127.0.0.1:40000", "join", "127.0.0.1:43000")

        if "OK" not in ctl:
            router.terminate()
            kvs.terminate()
            sys.exit(1)
        
        keys = random.sample(range(1, 1000), 100)
        for k in keys:
            ctl = run_ctl("127.0.0.1:40000", "put", f"{k} 2")
            if "OK" not in ctl:
                router.terminate()
                kvs.terminate()
                sys.exit(1)
        
        for k in keys:
            ctl = run_ctl("127.0.0.1:40000", "get", f"{k}")
            if "Value:\t2" not in ctl:
                router.terminate()
                kvs.terminate()
                sys.exit(1)
        
        for k in random.sample(range(1001, 1500), 100):
            ctl = run_ctl("127.0.0.1:40000", "get", f"{k}")
            if "Value:\tERROR" not in ctl:
                router.terminate()
                kvs.terminate()
                sys.exit(1)
        
        router.terminate()
        kvs.terminate()

        sys.exit(0)

if __name__ == "__main__":
    main()