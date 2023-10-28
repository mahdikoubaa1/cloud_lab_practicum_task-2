#!/usr/bin/env python3

import sys, random, os, signal
from time import sleep
from testsupport import subtest, run
from socketsupport import run_router, run_kvs, run_ctl

def main() -> None:
    with subtest("Testing simple kvs with one shard"):
        router = run_router("127.0.0.1:40000", "127.0.0.1:41000")
        kvs1    = run_kvs("127.0.0.1:42000", "127.0.0.1:43000", "127.0.0.1:41000")
        kvs2    = run_kvs("127.0.0.1:44000", "127.0.0.1:45000", "127.0.0.1:41000")

        sleep(5)

        ctl    = run_ctl("127.0.0.1:40000", "join", "127.0.0.1:43000")
        if "OK" not in ctl:
            run(["kill", "-9", str(router.pid)])
            run(["kill", "-9", str(kvs1.pid)])
            run(["kill", "-9", str(kvs2.pid)])
            sys.exit(1)
        
        ctl    = run_ctl("127.0.0.1:40000", "join", "127.0.0.1:45000")
        if "OK" not in ctl:
            run(["kill", "-9", str(router.pid)])
            run(["kill", "-9", str(kvs1.pid)])
            run(["kill", "-9", str(kvs2.pid)])
            sys.exit(1)
        
        sleep(5)
        
        keys = random.sample(range(1, 21), 20)
        for k in keys:
            ctl = run_ctl("127.0.0.1:40000", "put", f"{k} 2")
            if "OK" not in ctl:
                run(["kill", "-9", str(router.pid)])
                run(["kill", "-9", str(kvs1.pid)])
                run(["kill", "-9", str(kvs2.pid)])
                sys.exit(1)
        
        for k in keys:
            ctl = run_ctl("127.0.0.1:40000", "get", f"{k}")
            if "Value:\t2" not in ctl:
                run(["kill", "-9", str(router.pid)])
                run(["kill", "-9", str(kvs1.pid)])
                run(["kill", "-9", str(kvs2.pid)])
                sys.exit(1)
        
        sleep(5)
        run(["kill", "-9", str(kvs1.pid)])
        atleast_one_fail = False
        atleast_one_succeed = False

        sleep(5)

        for k in keys:
            ctl = run_ctl("127.0.0.1:40000", "get", f"{k}")
            print(ctl)
            if "Value:\t2" in ctl:
                atleast_one_succeed = True
            if "ERROR" in ctl:
                atleast_one_fail = True
        
        if not (atleast_one_fail and atleast_one_succeed):
            print("First Check failed")
            run(["kill", "-9", str(router.pid)])
            run(["kill", "-9", str(kvs1.pid)])
            run(["kill", "-9", str(kvs2.pid)])
            sys.exit(1)
        
        run(["kill", "-9", str(router.pid)])
        run(["kill", "-9", str(kvs1.pid)])
        run(["kill", "-9", str(kvs2.pid)])

        # Second run
        sleep(10)
        router = run_router("127.0.0.1:40000", "127.0.0.1:41000")
        kvs1    = run_kvs("127.0.0.1:42000", "127.0.0.1:43000", "127.0.0.1:41000")
        kvs2    = run_kvs("127.0.0.1:44000", "127.0.0.1:45000", "127.0.0.1:41000")

        sleep(5)

        ctl    = run_ctl("127.0.0.1:40000", "join", "127.0.0.1:43000")
        if "OK" not in ctl:
            run(["kill", "-9", str(router.pid)])
            run(["kill", "-9", str(kvs1.pid)])
            run(["kill", "-9", str(kvs2.pid)])
            sys.exit(1)
        
        ctl    = run_ctl("127.0.0.1:40000", "join", "127.0.0.1:45000")
        if "OK" not in ctl:
            run(["kill", "-9", str(router.pid)])
            run(["kill", "-9", str(kvs1.pid)])
            run(["kill", "-9", str(kvs2.pid)])
            sys.exit(1)
        
        sleep(5)
        
        keys = random.sample(range(1, 21), 20)
        for k in keys:
            ctl = run_ctl("127.0.0.1:40000", "put", f"{k} 2")
            if "OK" not in ctl:
                run(["kill", "-9", str(router.pid)])
                run(["kill", "-9", str(kvs1.pid)])
                run(["kill", "-9", str(kvs2.pid)])
                sys.exit(1)
        
        for k in keys:
            ctl = run_ctl("127.0.0.1:40000", "get", f"{k}")
            if "Value:\t2" not in ctl:
                run(["kill", "-9", str(router.pid)])
                run(["kill", "-9", str(kvs1.pid)])
                run(["kill", "-9", str(kvs2.pid)])
                sys.exit(1)
        
        sleep(5)
        run(["kill", "-9", str(kvs2.pid)])
        atleast_one_fail = False
        atleast_one_succeed = False

        sleep(5)

        for k in keys:
            ctl = run_ctl("127.0.0.1:40000", "get", f"{k}")
            print(ctl)
            if "Value:\t2" in ctl:
                atleast_one_succeed = True
            if "ERROR" in ctl:
                atleast_one_fail = True
        
        if not (atleast_one_fail and atleast_one_succeed):
            print("Second Check failed")
            run(["kill", "-9", str(router.pid)])
            run(["kill", "-9", str(kvs1.pid)])
            run(["kill", "-9", str(kvs2.pid)])
            sys.exit(1)

        run(["kill", "-9", str(router.pid)])
        run(["kill", "-9", str(kvs1.pid)])
        run(["kill", "-9", str(kvs2.pid)])

        sys.exit(0)



if __name__ == "__main__":
    main()