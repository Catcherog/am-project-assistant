"""Command entry point; production transports are intentionally not started."""
from .cli import main


raise SystemExit(main())
