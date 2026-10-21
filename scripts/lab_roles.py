"""Sets database role passwords from your .env, so no password sits in a SQL file or in git.

Source: ~/de_lab/lab_kit/lab_roles.py. new_project.sh copies it into scripts/ of each repo.
Usage:  python scripts/lab_roles.py --admin-prefix PG7 router_app=PG7_APP_PASSWORD ...
Each ROLE=ENV_VAR pair sets ALTER ROLE ROLE PASSWORD to the value of ENV_VAR.
The admin login comes from PREFIX_USER, PREFIX_PASSWORD, PREFIX_HOST, PREFIX_PORT, PREFIX_DATABASE.
format('%L') quotes the password as a SQL literal, so a quote inside it cannot break out.
"""

import argparse
import os
import sys

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

load_dotenv(os.path.expanduser("~/de_lab/.env"))


def admin_engine(prefix):
    url = URL.create(
        "postgresql+psycopg2",
        username=os.environ[f"{prefix}_USER"],
        password=os.environ[f"{prefix}_PASSWORD"],
        host=os.environ.get(f"{prefix}_HOST", "localhost"),
        port=int(os.environ.get(f"{prefix}_PORT", "5432")),
        database=os.environ.get(f"{prefix}_DATABASE", "de_lab"),
    )
    return create_engine(url)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--admin-prefix", required=True)
    ap.add_argument("pairs", nargs="+", help="ROLE=ENV_VAR")
    a = ap.parse_args(argv)
    with admin_engine(a.admin_prefix).begin() as conn:
        for pair in a.pairs:
            role, _, var = pair.partition("=")
            pw = os.environ.get(var, "")
            if not role or not pw:
                print(f"missing role name or empty {var or pair} in .env", file=sys.stderr)
                return 1
            stmt = conn.execute(
                text(
                    "SELECT format('ALTER ROLE %I PASSWORD %L', CAST(:r AS text), CAST(:p AS text))"
                ),
                {"r": role, "p": pw},
            ).scalar_one()
            conn.execute(text(stmt))
            print(f"password set for role {role}")
    return 0


if __name__ == "__main__":
    sys.exit(main())