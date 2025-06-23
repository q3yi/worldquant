import argparse
import os
import sys
import time

import brain

from alpha_db import AlphaDB


def fetch_results(db: AlphaDB, cli: brain.Client):
    simulations = db.simulations()
    alphas = db.alphas()
    total = 0
    wait_sec = 1.0
    while True:
        count = 0
        for row in simulations.filter(status="SIMULATING"):
            try:
                result = cli.simulation_result(row["simulation_id"])

                alpha = result.wait().detail()
                count += 1

                alphas.save(alpha)
                simulations.complete(row["id"], alpha["id"])

                print(f"{count:0>3}, save alpha: {alpha['id']}")
            except brain.BrainError as e:
                simulations.error(row["id"])
                print(
                    f"{count:0>3}, simulation: {row['simulation_id']}, error: {str(e)}",
                    file=sys.stderr,
                )

        if count != 0:
            total += count
            wait_sec = wait_sec / 3.0 if wait_sec / 3.0 > 1.0 else 1.0

            print(
                f"[{total:0>4}] saved {count} alphas, wait {wait_sec:.2f} secs for next scan."
            )
        else:
            wait_sec = wait_sec * 2 if wait_sec < 5.0 else wait_sec
            print(
                f"[{total:0>4}] no simulations found, wait {wait_sec:.2f} secs for next scan."
            )

        time.sleep(wait_sec)


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
