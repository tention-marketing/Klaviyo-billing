#!/usr/bin/env python3
"""
Create or update the database schema. Safe to run repeatedly.

    python3 migrate.py

Every statement is guarded (CREATE ... IF NOT EXISTS or a DO block), so this
never drops or rewrites data.
"""

import sys

import db


def main() -> int:
    if not db.is_configured():
        print("DATABASE_URL is not set. Nothing to migrate.")
        print("Set it to your Railway Postgres URL and run this again.")
        return 1

    print("Migrating {} ...".format(db.safe_target()))
    try:
        for name, status in db.migrate():
            print("  {:<38} {}".format(name, status))
    except db.DatabaseUnavailable as exc:
        print("ERROR: {}".format(exc), file=sys.stderr)
        return 1

    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
