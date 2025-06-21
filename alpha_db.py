import sqlite3
import json


class AlphaDB:
    def __init__(self, dbfile: str):
        self.dbfile = dbfile
        self._conn = None

    def __enter__(self):
        self._conn = sqlite3.connect(self.dbfile)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._conn.close()

    def fields(self):
        return Fields(self._conn)

    def simulations(self):
        return Simulations(self._conn)

    def alphas(self):
        return Alphas(self._conn)


CREATE_FIELDS_TABLE = """CREATE TABLE IF NOT EXISTS fields(
    id TEXT PRIMARY KEY,
    type TEXT NOT NULL,
    dataset_id TEXT NOT NULL,
    category_id TEXT,
    subcategroy_id TEXT,
    universe TEXT NOT NULL,
    region TEXT,
    delay INTEGER,
    description TEXT
)
"""

INSERT_FIELDS_TABLE = """INSERT INTO fields
    VALUES(:id, :type, :dataset_id, :category_id, :subcategroy_id, :universe, :region, :delay, :description)"""


class Fields:
    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn
        self._init_table()

    def _init_table(self):
        cursor = self._conn.cursor()
        cursor.execute(CREATE_FIELDS_TABLE)
        self._conn.commit()

    def from_brain_resp(self, field: dict):
        return {
            "id": field["id"],
            "type": field["type"],
            "dataset_id": field["dataset"]["id"],
            "category_id": field["category"]["id"],
            "subcategroy_id": field["subcategory"]["id"],
            "universe": field["universe"],
            "region": field["region"],
            "delay": int(field["delay"]),
            "description": field["description"],
        }

    def insert_many(self, fields_list: [dict]):
        cursor = self._conn.cursor()
        cursor.executemany(INSERT_FIELDS_TABLE, map(self.from_brain_resp, fields_list))
        self._conn.commit()

    def filter(self, data_type: str):
        cursor = self._conn.cursor()
        cursor.execute("SELECT * FROM fields WHERE type = ?", (data_type,))
        for row in cursor:
            yield {
                "id": row[0],
                "type": row[1],
                "dataset_id": row[2],
                "category_id": row[3],
                "subcategroy_id": row[4],
                "universe": row[5],
                "region": row[6],
                "delay": row[7],
                "description": row[8],
            }


CREATE_SIMULATION_TABLE = """CREATE TABLE IF NOT EXISTS simulations(
    id INTEGER PRIMARY KEY,
    expr TEXT NOT NULL,
    type TEXT NOT NULL,
    settings TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at INTEGER DEFAULT (UNIXEPOCH()),
    simulated_at INTERGER,
    simulation_id TEXT,
    completed_at INTERGER,
    alpha_id TEXT
)"""


class Simulations:
    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn
        self._init_table()

    def _init_table(self):
        cursor = self._conn.cursor()
        cursor.execute(CREATE_SIMULATION_TABLE)
        self._conn.commit()

    def filter(self, status: str = "PENDING"):
        cursor = self._conn.cursor()
        cursor.execute("SELECT * FROM simulations WHERE status = ?", (status,))

        for row in cursor.fetchall():
            yield {
                "id": row[0],
                "expr": row[1],
                "type": row[2],
                "settings": json.loads(row[3]),
                "status": row[4],
                "created_at": row[5],
                "simulated_at": row[6],
                "simulation_id": row[7],
                "completed_at": row[8],
                "alpha_id": row[9],
            }

    def start(self, id: int, simulation_id: str):
        cursor = self._conn.cursor()
        cursor.execute(
            "UPDATE simulations SET status = 'SIMULATING', simulated_at = UNIXEPOCH(), simulation_id = ? WHERE id = ?",
            (
                simulation_id,
                id,
            ),
        )
        self._conn.commit()

    def complete(self, id: int, alpha: str):
        cursor = self._conn.cursor()
        cursor.execute(
            "UPDATE simulations SET status = 'COMPLETE', completed_at = UNIXEPOCH(), alpha_id = ? WHERE id = ?",
            (
                alpha,
                id,
            ),
        )
        self._conn.commit()

    def error(self, id: int):
        cursor = self._conn.cursor()
        cursor.execute(
            "UPDATE simulations SET status = 'ERROR', completed_at = UNIXEPOCH() WHERE id = ?",
            (id,),
        )
        self._conn.commit()


CREATE_ALPHA_TABLE = """CREATE TABLE IF NOT EXISTS alphas (
    id TEXT PRIMARY KEY,
    settings TEXT,
    status TEXT,
    grade TEXT,
    stage TEXT,
    is_summary TEXT,
    train TEXT,
    test TEXT,
    checks TEXT
)"""

INSERT_ALPHA_TABLE = """INSERT INTO alphas(id, settings, status, grade, stage, is_summary, train, test, checks) VALUES(?,?,?,?,?,?,?,?,?)"""


class Alphas:
    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn
        self._init_table()

    def _init_table(self):
        cursor = self._conn.cursor()
        cursor.execute(CREATE_ALPHA_TABLE)
        self._conn.commit()

    def save(self, alpha: dict):
        checks = alpha["is"].get("checks")
        check_flag = not any([x.get("result", "") == "FAIL" for x in checks])

        cursor = self._conn.cursor()
        cursor.execute(
            INSERT_ALPHA_TABLE,
            (
                alpha["id"],
                json.dumps(alpha["settings"]),
                alpha["status"],
                alpha["grade"],
                alpha["stage"],
                json.dumps(alpha["is"]),
                json.dumps(alpha["train"]),
                json.dumps(alpha["test"]),
                "PASS" if check_flag else "FAIL",
            ),
        )
        self._conn.commit()
