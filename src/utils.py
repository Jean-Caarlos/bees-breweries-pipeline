import time
import logging
import requests

log = logging.getLogger(__name__)

def get_with_retry(url, params=None, max_retries=5, backoff=1.5, timeout=15):
    for attempt in range(1, max_retries+1):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            if r.status_code == 200:
                return r.json()
            log.warning(f"Status {r.status_code} on {url} params={params}")
        except requests.RequestException as e:
            log.warning(f"Attempt {attempt} failed: {e}")
        time.sleep(backoff**attempt)
    raise RuntimeError(f"Failed to GET {url} after {max_retries} attempts")
