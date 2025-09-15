import requests, datetime, json
from smart_open import open as sm_open
from tqdm import tqdm
from ..aws import s3_client, write_success
from ..config import BUCKET

BASE_S3_PREFIX = "bronze/hmlr/price-paid/yearly_txt/"

def ingest_yearly_txt():
    urls = {
        "2018": "http://prod2.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-2018.txt",
        "2019": "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-2019.txt",
        "2020": "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-2020.txt",
        "2021": "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-2021.txt",
        "2022": "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-2022.txt",
        "2023": "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-2023.txt",
        "2024": "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-2024.txt",
        "2025": "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-2025.txt",
    }
    results = []
    for year, url in urls.items():
        s3_key = BASE_S3_PREFIX + f"pp-{year}.txt"
        with requests.get(url, stream=True, timeout=60) as r:
            r.raise_for_status()
            with sm_open(f"s3://{BUCKET}/{s3_key}", 'wb', transport_params={'client': s3_client()}) as f:
                for chunk in r.iter_content(chunk_size=8*1024*1024):
                    if chunk:
                        f.write(chunk)
        results.append({"year": year, "s3_key": s3_key})
    write_success(BUCKET, BASE_S3_PREFIX + "_SUCCESS")
    return {
        "source": "hmlr_yearly_txt",
        "ingest_ts": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "bucket": BUCKET,
        "objects": results
    }
