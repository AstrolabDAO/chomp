from actions.store import store
from actions.transform import transform
import requests
from bs4 import BeautifulSoup
from typing import Dict, Optional
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional, Any
from ..model import CollectorConfig, ResourceField, TsdbAdapter

def collect(ctx, db: TsdbAdapter, config: CollectorConfig):
  # Fetched HTML content to avoid refetching the same page
  page_cache: Dict[str, str] = {}

  # Iterate over each resource field defined in the collector configuration
  for field in config.data:
    url = field.target
    if url:
      # Fetch and cache the page content if not already fetched
      if url not in page_cache:
        response = requests.get(url)
        if response.status_code == 200:
          page_cache[url] = response.text
        else:
          print(f"Failed to fetch {url}")
          continue

      # Parse the HTML content
      soup = BeautifulSoup(page_cache[url], 'html.parser')

      # Extract data using the CSS selector
      elements = soup.select(field.selector)
      field.value = None if not elements else elements[0].get_text()
      if field.value and field.transformers:
        transform(db, config, field)
        field.value = transform(db, config, field)
  store(db, config)
