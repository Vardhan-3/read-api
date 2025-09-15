import hashlib, datetime, json
from smart_open import open as sm_open
from firecrawl import FirecrawlApp
from ..aws import s3_client, write_success
from ..config import BUCKET, FIRECRAWL_KEY

PREFIX = "bronze/firecrawl/uk-local-news/"

def ingest_news():
    app  = FirecrawlApp(api_key=FIRECRAWL_KEY)
    urls = [
        "https://www.rightmove.co.uk/news/",
        "https://www.zoopla.co.uk/discover/property-news/"
    ]
    results = []
    for url in urls:
        out = app.scrape(url, formats=["markdown"])
        md  = out.markdown or ""
        slug = hashlib.md5(url.encode()).hexdigest()[:8]
        key  = f"{PREFIX}{datetime.datetime.now(datetime.timezone.utc).isoformat()[:10]}/{slug}.json"
        payload = {
            "url": url,
            "crawl_ts": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "markdown": md,
            "metadata": out.metadata.model_dump() if out.metadata else {}
        }
        with sm_open(f"s3://{BUCKET}/{key}", 'w', transport_params={'client': s3_client()}) as f:
            f.write(json.dumps(payload, ensure_ascii=False, indent=2))
        results.append({"url": url, "s3_key": key, "len_md": len(md)})
    write_success(BUCKET, PREFIX+"_SUCCESS")
    return {
        "source": "firecrawl_rm_zoopla",
        "ingest_ts": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "bucket": BUCKET,
        "crawls": results
    }
