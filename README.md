# CLI T212 to Digrin
Python CLI tool for fetching T212 reports via API call and transforming them to be used in Digrin portfolio tracker. Stores the reports in AWS S3.

```
echo "T212_API_KEY=$T212_API_KEY" >> .env
echo "AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID" >> .env
echo "AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY" >> .env
```

```
uv run main.py
```

# TODO

- [ ] archive reports in parquet ?

- [ ] add type hints for created variables in main()

- [ ] add logging

- [ ] add tests

- [ ] investigate option of asyncio
