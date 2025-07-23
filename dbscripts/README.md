# Database Scripts

Collection of postgresql scripts that create intermediate tables used by downstream dataprocessing code

## Order of Operations

```bash
psql -f bcresults.sql

# Depends on bcresults
python compile_sa.py > tokenize.sql
psql -f tokenize.sql # Long runtime

# No non-mimic dependencies, can run in any order
psql -f splits.sql
psql -f staticfeats.sql
psql -f overnightblood.sql
```