# World Quant Brain alpha utils

Uitls for mining alphas with world quant brain API. It has three commands:

1. `crawl.py`: Crawling fields from world quant brain API.
2. `simulate.py`: Send simulations to brain API.
3. `collect.py`: Collect simulation results from brain API.

Fields, simulations and alphas all stored in a local sqlite3 DB.

Examples:

1. Crawling fields

```bash
python crawl.py --db alpha.db --type MATRIX --dataset_id fundamental6
```

2. Generate simulation configs with sql in db

3. Send simulation to API

```bash
python simulate.py --db alpha.db --limit 100 --interval 60
```

4. Collect simulation results and alpha at same time

```bash
python collect.py --db alpha.db --interval 60
```
