import argparse
import os
import sys
import time

import brain

from alpha_db import AlphaDB


def print_succ(idx: int, expr: str, msg: str):
    if len(expr) > 50:
        expr = expr[:47] + "..."

    print(f"[{idx:0>3}][\33[0;32mSUCC\033[0m] {expr:<50} {msg}")


def print_erro(idx: int, expr: str, msg: str):
    if len(expr) > 50:
        expr = expr[:47] + "..."

    print(f"[{idx:0>3}][\33[0;31mERRO\033[0m] {expr:<50} {msg}")


class RateLimiter:
    def __init__(self):
        self.wait_secs = 1.0
        self.succ_streak = 0
        self.fail_streak = 0

    def succ(self):
        if self.fail_streak:
            self.wait_secs /= 2.0
            self.fail_streak = 0
        else:
            self.succ_streak += 1

        if self.succ_streak >= 5:
            factor = 0.02 * float(self.succ_streak) if self.succ_streak < 10 else 0.14
            self.wait_secs = self.wait_secs * (0.8 + factor)

        return self.wait_secs

    def fail(self):
        self.succ_streak = 0
        self.fail_streak += 1
        self.wait_secs *= 2.1

        return self.wait_secs

    def wait(self):
        time.sleep(self.wait_secs)


def simulate(db: AlphaDB, sim: brain.Simulation, limit: int):
    guard = RateLimiter()
    simulations = db.simulations()
    for idx, row in enumerate(simulations.filter(status="PENDING"), start=1):
        while True:
            try:
                result = (
                    sim.with_type(row["type"])
                    .with_settings(**row["settings"])
                    .with_expr(row["expr"])
                    .send()
                )

                simulations.start(row["id"], result.simulation_id)

                print_succ(idx, row["expr"], f"Next after {guard.succ():.2f} secs.")
                guard.wait()

                break
            except brain.BrainError:
                print_erro(idx, row["expr"], f"Retry after {guard.fail():.2f} secs.")
                guard.wait()

        if limit != 0 and idx >= limit:
            break


def main():
    parser = argparse.ArgumentParser(description="Send alphas to Brain simulation API.")
    parser.add_argument(
        "--user",
        default=os.environ.get("WQB_USER"),
        help="Brain API user. use env WQB_USER if not given.",
    )
    parser.add_argument(
        "--password",
        default=os.environ.get("WQB_PASS"),
        help="Brain API password. use env WQB_PASS if not given.",
    )
    parser.add_argument(
        "--db", default="alpha.db", help="sqlite db that store all simulations."
    )
    parser.add_argument(
        "--limit", default=0, type=int, help="max number of alpha to send."
    )

    args = parser.parse_args(sys.argv[1:])

    if not args.user or not args.password:
        print("no user or password found.", file=sys.stderr)
        sys.exit(1)

    cli = brain.Client(args.user, args.password)
    sim = cli.simulation()

    with AlphaDB(args.db) as db:
        simulate(db, sim, args.limit)


if __name__ == "__main__":
    main()
