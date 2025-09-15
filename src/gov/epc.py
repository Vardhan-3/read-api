import requests, datetime
from smart_open import open as sm_open
from tqdm import tqdm
from ..aws import s3_client, write_success
from ..config import BUCKET, EPC_TOKEN

EPC_URL = "https://epc.opendatacommunities.org/api/v1/files/all-domestic-certificates.zip"
HEADERS = {"Authorization": f"Basic {EPC_TOKEN}"}
S3_KEY  = "bronze/epc/domestic/epc-domestic-all-certificates.zip"

def ingest_domestic_zip():
    resp = requests.get(EPC_URL, headers=HEADERS, stream=True, timeout=60)
    resp.raise_for_status()
    size = int(resp.headers.get('content-length', 0))
    with sm_open(f"s3://{BUCKET}/{S3_KEY}", 'wb', transport_params={'client': s3_client()}) as f:
        for chunk in tqdm(resp.iter_content(chunk_size=8*1024*1024), total=size//(8*1024*1024), unit='MB'):
            if chunk:
                f.write(chunk)
    write_success(BUCKET, "bronze/epc/domestic/_SUCCESS")
    return {
        "source": "epc_domestic_register",
        "ingest_ts": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "url": EPC_URL,
        "bucket": BUCKET,
        "s3_key": S3_KEY,
        "size_bytes": size
    }
