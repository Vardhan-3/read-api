# read-api

data-ingest/
├── README.md
├── requirements.txt
├── src/
│   ├── __init__.py
│   ├── config.py
│   ├── aws.py
│   ├── gov/
│   │   ├── __init__.py
│   │   ├── hmlr.py
│   │   └── epc.py
│   └── crawl/
│       ├── __init__.py
│       └── firecrawl_rm_zoopla.py
├── logs/
│   └── .gitkeep
└── run_ingest.py            # <-- entry point
