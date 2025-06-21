import argparse
import os
import sys
import time

import brain
import alpha_db


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
    parser.add_argument(
        "--interval",
        default=10.0,
        type=float,
        help="wait time before scan uncollected simulations in database.",
    )

    args = parser.parse_args(sys.argv[1:])

    if not args.user or not args.password:
        print("no user or password found.", file=sys.stderr)
        sys.exit(1)

    cli = brain.Client(args.user, args.password)

    with alpha_db.AlphaDB(args.db) as db:
        simulations = db.simulations()
        alphas = db.alphas()
        total = 0
        while True:
            count = 0
            for row in simulations.filter(status="SIMULATING"):
                try:
                    result = cli.simulation_result(row["simulation_id"])

                    alpha = result.wait().detail()
                    count += 1

                    alphas.save(alpha)
                    simulations.complete(row["id"], alpha["id"])

                    print("{:0>3}, save alpha: {}".format(count, alpha["id"]))
                except brain.BrainError as e:
                    simulations.error(row["id"])
                    print(
                        "{:0>3}, simulation: {}, error: {}".format(
                            count + 1, row["simulation_id"], str(e)
                        ),
                        file=sys.stderr,
                    )

            if count != 0:
                total += count
                print(
                    "[{:0>4}] {:0>3} alpha saved, wait for next scan.".format(
                        total, count
                    )
                )
            else:
                print(
                    "[{:0>4}] no simulations found, waiting for next scan.".format(
                        total
                    )
                )
            time.sleep(args.interval)


if __name__ == "__main__":
    main()
