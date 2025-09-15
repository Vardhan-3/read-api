import os
from google.colab import userdata   # works in Colab; fallback env vars elsewhere

AWS_KEY    = os.getenv("AWS_ACCESS_KEY_ID")    or userdata.get('AWS_ACCESS_KEY_ID')
AWS_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY") or userdata.get('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv("AWS_DEFAULT_REGION")   or "eu-west-1"
BUCKET     = os.getenv("S3_BUCKET")            or "uk-property-bronze"

FIRECRAWL_KEY = os.getenv("FIRECRAWL_API_KEY") or userdata.get('FIRECRAWL_API_KEY')
EPC_TOKEN     = os.getenv("EPC_API_KEY")       or userdata.get('EPC_API_KEY')
