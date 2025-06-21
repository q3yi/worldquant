import argparse
import os
import sys
import time

import brain
import alpha_db


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
    parser.add_argument(
        "--interval", default=10.0, type=float, help="sleep time between two request."
    )

    args = parser.parse_args(sys.argv[1:])

    if not args.user or not args.password:
        print("no user or password found.", file=sys.stderr)
        sys.exit(1)

    cli = brain.Client(args.user, args.password)
    sim = cli.simulation()

    with alpha_db.AlphaDB(args.db) as db:
        simulations = db.simulations()
        count = 0
        for row in simulations.filter(status="PENDING"):
            try:
                result = (
                    sim.with_type(row["type"])
                    .with_settings(**row["settings"])
                    .with_expr(row["expr"])
                    .send()
                )
                count += 1

                simulations.start(row["id"], result.simulation_id)
                print("{:0>3}, simulate expr: {}".format(count, row["expr"]))
            except brain.BrainError as e:
                print(
                    "{:0>3}, simulate expr: {}, error: {}".format(
                        count + 1, row["expr"], str(e)
                    ),
                    file=sys.stderr,
                )

            if args.limit != 0 and count >= args.limit:
                break

            time.sleep(args.interval)


if __name__ == "__main__":
    main()
