import argparse
import os
import sys
import time

import brain

from alpha_db import AlphaDB


def fetch_results(db: AlphaDB, cli: brain.Client):
    simulations = db.simulations()
    alphas = db.alphas()

    succ, fail, wait_sec = 0, 0, 1.0

    def print_info(msg: str, file=sys.stdout):
        print(
            f"[\33[0;32m{succ:0>4}\033[0m|\33[0;31m{fail:0>4}\033[0m] {msg}", file=file
        )

    while True:
        is_empty = True
        for row in simulations.filter(status="SIMULATING"):
            try:
                is_empty = False

                alpha = cli.simulation_result(row["simulation_id"]).wait().detail()
                alphas.save(alpha)
                simulations.complete(row["id"], alpha["id"])

                succ += 1
                print_info(f"New alpha: {alpha['id']}")
            except brain.BrainError as e:
                simulations.error(row["id"])
                fail += 1
                print_info(
                    f"Simulation: {row['simulation_id']}, Error: {str(e)}", sys.stderr
                )

        if is_empty:
            wait_sec = wait_sec * 2 if wait_sec < 5.0 else wait_sec
        else:
            wait_sec = wait_sec / 3.0 if wait_sec > 1.0 else 1.0

        print_info(f"Rescan after {wait_sec:.2f} secs.")

        time.sleep(wait_sec)

        print_info("Rescan...")


def main():
    parser = argparse.ArgumentParser(description="Get simulated alphas from Brain API.")
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
        "--db", default="alpha.db", help="sqlite db that store all alphas."
    )
    parser.add_argument(
        "--limit", default=0, type=int, help="max number of alphas to get."
    )

    args = parser.parse_args(sys.argv[1:])

    if not args.user or not args.password:
        print("no user or password found.", file=sys.stderr)
        sys.exit(1)

    cli = brain.Client(args.user, args.password)

    with AlphaDB(args.db) as db:
        fetch_results(db, cli)


if __name__ == "__main__":
    main()
