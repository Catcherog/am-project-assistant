import argparse
import json
from contextlib import closing
from pathlib import Path
import sqlite3
from .metrics import report
from .collection_service import read_coverage


class ReadOnlyStore:
    def __init__(self,path):
        self.uri=Path(path).resolve().as_uri()+"?mode=ro"
    def rows(self,sql,args=()):
        with closing(sqlite3.connect(self.uri,uri=True)) as db:
            db.row_factory=sqlite3.Row
            return [dict(r) for r in db.execute(sql,args)]
    def one(self,sql,args=()):
        rows=self.rows(sql,args)
        return rows[0] if rows else None


def main():
    parser=argparse.ArgumentParser(description="AM multi-AI gateway diagnostics; does not start a consumer")
    parser.add_argument("command",choices=["metrics","jobs","outbox","coverage"])
    parser.add_argument("--db",required=True)
    args=parser.parse_args()
    store=ReadOnlyStore(args.db)
    if args.command=="metrics":
        value=report(store)
    elif args.command=="coverage":
        value=read_coverage(store)
    else:
        value=store.rows("SELECT * FROM gw_"+args.command+" ORDER BY created_at DESC LIMIT 100")
    print(json.dumps(value,ensure_ascii=False,indent=2))


if __name__=="__main__": main()
