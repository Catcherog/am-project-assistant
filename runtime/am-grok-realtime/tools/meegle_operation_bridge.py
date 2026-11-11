"""Invoke the configured Meegle executor; business data arrives only on stdin."""
from pathlib import Path
import json
import sys
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'src'),str(ROOT)]
from integration.meegle_executor import main
if __name__=='__main__':
    try:main()
    except Exception as exc:
        print(json.dumps({'status':'error','error_type':type(exc).__name__}));raise SystemExit(1)
