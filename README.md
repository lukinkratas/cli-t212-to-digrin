# CLI T212 to Digrin
Python CLI tool for fetching T212 reports and transforming them to be used in Digrin portfolio tracker.

## Install
```
pip install uv
uv run main.py
```

## Usage
```
python3 main.py --from 2025-02-01 --to 2025-02-28
python3 main.py --month 2025-02
python3 main.py # ask for use input
-d -- download for local donwload?
-c --cloud for aws upload or maybe -a --aws?
```

## Ruff check