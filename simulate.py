import argparse
import os
import sys
import time

import brain

from alpha_db import AlphaDB


def simulate(db: AlphaDB, sim: brain.Simulation, limit: int):
    total, wait_sec, streak = 0, 1.0, 0
    simulations = db.simulations()
    for row in simulations.filter(status="PENDING"):
        retry = 0
        while True:
            try:
                result = (
                    sim.with_type(row["type"])
                    .with_settings(**row["settings"])
                    .with_expr(row["expr"])
                    .send()
                )
                total += 1

                simulations.start(row["id"], result.simulation_id)

                if retry:
                    # half wait time
                    wait_sec /= 2.0
                    streak = 0
                else:
                    streak += 1

                if streak >= 5:
                    wait_sec *= 0.9

                print(
                    f"[{total:0>3}][\33[0;32mDONE\033[0m] Expr: {row['expr']}, next after {wait_sec:.2f} secs."
                )

                time.sleep(wait_sec)
                break
            except brain.BrainError:
                wait_sec *= 2.1
                expr = f"[{total + 1:0>3}][\33[0;31mERRO\033[0m] Expr: {row['expr']}"

                # print(f"{expr}, error: {str(e)}", file=sys.stderr)
                print(f"{expr}, retry[{retry}] after {wait_sec:.2f} seconds.")

                time.sleep(wait_sec)
                retry += 1

        if limit != 0 and total >= limit:
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
