import argparse
import os
import sys

import brain

from alpha_db import AlphaDB

# example = [
#     {
#         "id": "assets",
#         "description": "Assets - Total",
#         "dataset": {
#             "id": "fundamental6",
#             "name": "Company Fundamental Data for Equity",
#         },
#         "category": {"id": "fundamental", "name": "Fundamental"},
#         "subcategory": {
#             "id": "fundamental- fundamental-data",
#             "name": "Fundamental Data",
#         },
#         "region": "USA",
#         "delay": 1,
#         "universe": "TOP3000",
#         "type": "MATRIX",
#         "coverage": 0.9524,
#         "userCount": 23894,
#         "alphaCount": 80966,
#         "themes": [],
#     }
# ]


def main():
    parser = argparse.ArgumentParser(
        description="Crawling fields data from Brain simulation API."
    )
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
        "--chunk_size",
        default=50,
        type=int,
        help="require numbers of fields in one request.",
    )
    parser.add_argument(
        "--limit", default=None, type=int, help="max numbers of fields to fetch."
    )
    parser.add_argument("--universe", default="TOP3000", help="fields filter: universe")
    parser.add_argument(
        "--instrument_type", default="EQUITY", help="fields filter: instrumentType"
    )
    parser.add_argument("--delay", default=1, type=int, help="fields filter: delay")
    parser.add_argument("--region", default="USA", help="fields filter: region")
    parser.add_argument("--type", default=None, help="fields filter: type")
    parser.add_argument("--dataset_id", default=None, help="fields filter: dataset.id")

    args = parser.parse_args(sys.argv[1:])

    if not args.user or not args.password:
        print("no user or password found.", file=sys.stderr)
        sys.exit(1)

    cli = brain.Client(args.user, args.password)

    fields = cli.data_fields().with_filter(chunk_size=args.chunk_size)
    fields = fields.with_filter(
        universe=args.universe,
        instrument_type=args.instrument_type,
        region=args.region,
        delay=args.delay,
    )

    if args.type is not None:
        fields = fields.with_filter(data_type=args.type)

    if args.dataset_id is not None:
        fields = fields.with_filter(dataset_id=args.dataset_id)

    if args.limit is not None:
        fields = fields.limit(args.limit)

    field_list = [x._content for x in fields.iter()]

    with AlphaDB(args.db) as db:
        fields = db.fields()
        fields.insert_many(field_list)

    print(f"{len(field_list)} fields imported.")


if __name__ == "__main__":
    main()
