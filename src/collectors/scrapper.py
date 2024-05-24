import asyncio
import requests
from hashlib import md5
from lxml import html
from bs4 import BeautifulSoup

from src.model import Collector
from src.utils import floor_utc, interval_to_seconds, log_error
from src.actions.store import store
from src.actions.transform import transform
from src.cache import ensure_claim_task, get_or_set_cache
import src.state as state

def is_xpath(selector: str) -> bool:
  return selector.startswith(("//", "./"))

def get_page(url: str) -> str:
  response = requests.get(url)
  if response.status_code == 200:
    return response.text
  log_error(f"Failed to fetch page {url}, status code: {response.status_code}")
  return ""

async def schedule(c: Collector) -> list[asyncio.Task]:

  soup_by_page: dict[str, BeautifulSoup] = {}
  tree_by_page: dict[str, html.HtmlElement] = {}

  async def collect(c: Collector):
    await ensure_claim_task(c)
    expiry_sec = interval_to_seconds(c.interval)
    for field in c.data:
      url = field.target
      if not url:
        log_error(f"Missing target URL for field scrapper {c.name}.{field.name}, skipping...")
        continue

      # Create a unique key using a hash of the URL and interval
      page_hash = md5(f"{url}:{c.interval}".encode()).hexdigest()
      page = await get_or_set_cache(page_hash, lambda: get_page(url), expiry_sec)
      if not page:
        log_error(f"Failed to fetch page {url}, skipping...")
        continue

      # Scrape either by CSS selector or XPath
      if is_xpath(field.selector):
        if page not in tree_by_page:
          tree_by_page[page_hash] = html.fromstring(page)
        elements = tree_by_page[page_hash].xpath(field.selector)
        if not elements or len(elements) == 0:
          log_error(f"Failed to find element {field.selector} in page {url}, skipping...")
          continue
        # merge all text content from matching selectors
        field.value = "\n".join([e.text_content().lstrip() for e in elements])
      else:
        if page not in soup_by_page:
          soup_by_page[page_hash] = BeautifulSoup(page, 'html.parser')
        elements = soup_by_page[page_hash].select(field.selector)
        if not elements or len(elements) == 0:
          log_error(f"Failed to find element {field.selector} in page {url}, skipping...")
          continue
        # merge all text content from matching selectors
        field.value = "\n".join([e.get_text().lstrip() for e in elements])

      # Apply transformations if any
      if field.value and field.transformers:
        field.value = transform(c, field)
      c.data_by_field[field.name] = field.value

    # reset local parser caches
    soup_by_page.clear()
    tree_by_page.clear()

    c.collection_time = floor_utc(c.interval) # round down to theoretical task time
    await store(c)

  # globally register/schedule the collector
  return [state.add_cron(c.id, fn=collect, args=(c,), interval=c.interval)]
