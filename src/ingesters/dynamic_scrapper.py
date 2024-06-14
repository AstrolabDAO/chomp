from asyncio import sleep, gather
import random
from typing import Any, Callable, List, Literal, Tuple
from playwright.async_api import async_playwright, ViewportSize, Page, Locator, Browser, Playwright, BrowserType, ElementHandle
from hashlib import md5
from src.utils import interval_to_seconds, log_error
from src.model import Ingester, ResourceField
from src.cache import ensure_claim_task, get_or_set_cache
import src.state as state
from src.actions import transform_and_store, scheduler

SelectorType = Literal[
  "auto", "css", "xpath",
  "id", "name", "class",
  "role", "strict_role",
  "value", "text", "strict_text",
  "alt_text", "strict_alt_text",
  "title", "strict_title",
  "label", "strict_label",
  "placeholder", "strict_placeholder"]

class Puppet:
  by_id: dict[str, "Puppet"] = {}

  ingester: Ingester
  field: ResourceField
  play: Playwright
  browser: Browser
  pages: list[Page] = []
  selector: Locator
  elements: list[ElementHandle] = []

  def __init__(self, field: str, ingester: Ingester, play: Playwright):
    Puppet.by_id[field.id] = self
    self.field = field
    self.ingester = ingester
    self.play = play

  async def from_field(field: ResourceField, ingester: Ingester, play: Playwright) -> "Puppet":
    p = Puppet(field, ingester, play)
    for a in field.actions:
      await p.act(a)
    return p

  async def kill(self):
    await self.browser.close() if self.browser else None
    await gather(*[p.close() for p in self.pages])

  async def ensure_browser(self, browser="chromium") -> Browser:
    if not self.browser:
      self.browser = await self.play[browser].launch()
    return self.browser

  async def ensure_page(self, target=None) -> Page:
    target = target or self.field.target or "about:blank"
    if not self.pages:
      await self.ensure_browser()
      pages = [await self.browser.new_page(target)]
    return pages[-1]

  async def ensure_selected(self, selector="html", by="auto") -> Locator:
    if not self.selected:
      await self.ensure_page()
      self.selector = await self.select(selector=selector, by=by)
      # self.elements = await self.selector.element_handles() #.query_all()
    return self.selected[-1]

  async def ensure_elements(self) -> list[ElementHandle]:
    if not self.elements:
      await self.ensure_selected()
      self.elements = await self.selector.element_handles()
    return self.elements

  async def ensure_contents(self) -> list[any]:
    if not self.elements:
      await self.ensure_elements()
    return gather(*[e.text_content() for e in self.elements])

  # TODO: add support for regex
  async def select(self, selector: str="html", by: SelectorType|any="auto") -> Locator:
    l: Locator
    match by:
      case "auto": l = self.ensure_page().locator(selector)
      case "css": l = self.ensure_page().locator(f"css={selector}")
      case "xpath": l = self.ensure_page().locator(f"xpath={selector}")
      case "id": l = self.ensure_page().locator(f"id={selector}")
      case "name": l = self.ensure_page().locator(f"name={selector}")
      case "class": l = self.ensure_page().locator(f"class={selector}")
      case "text" | "value": l = self.ensure_page().get_by_text(selector)
      case "strict_text" | "strict_value": l = self.ensure_page().locator(f"text={selector}")
      case "role": l = self.ensure_page().get_by_role(selector)
      case "strict_role": l = self.ensure_page().locator(f"[role={selector}]")
      case "alt_text": l = self.ensure_page().get_by_alt_text(selector)
      case "strict_alt_text": l = self.ensure_page().locator(f"[alt={selector}]")
      case "title": l = self.ensure_page().get_by_title(selector)
      case "strict_title": l = self.ensure_page().locator(f"title={selector}")
      case "label": l = self.ensure_page().get_by_label(selector)
      case "strict_label": l = self.ensure_page().locator(f"label={selector}")
      case "placeholder": l = self.ensure_page().get_by_placeholder(selector)
      case "strict_placeholder": l = self.ensure_page().locator(f"placeholder={selector}")
      case _:
        l = self.ensure_page().locator(f"[{by}={selector}]" if "data-" in by else f"[data-{by}={selector}]")
    self.selector = l
    return l

  async def act(self, action: str, *args):
    """Executes a state-changing action on the browser and returns the modified browser."""

    part, command = action.split(":")
    try:
      match part:
        case "browser":
          match command:
            case "use": # supports chromium, firefox, webkit, msedge (chromium based)
              self.browser = await self.play[args[0] if args else "chromium"].launch(*args[1:]) # --mute-audio --no-sandbox by default
            case "close":
              await self.browser.close() if self.browser else None
            case _:
              log_error(f"Unknown browser action: {action}")

        case "page":
          match command:
            case "new":
              self.pages.append(await self.browser.new_page())
            case "close":
              await self.pages.pop().close() if self.pages else None
            case "set_viewport_size":
              await self.ensure_page().set_viewport_size(ViewportSize(width=int(args[0]), height=int(args[1])))
            case "goto":
              await self.ensure_page().goto(args[0])
            case "go_back":
              await self.ensure_page().go_back()
            case "go_forward":
              await self.ensure_page().go_forward()
            case "reload":
              await self.ensure_page().reload()
            case "add_init_script":
              await self.ensure_page().add_init_script(args[0])
            case "evaluate":
              await self.ensure_page().evaluate(args[0])
            case "evaluate_handle":
              await self.ensure_page().evaluate_handle(args[0])
            case "select":
              await self.ensure_page().select(args[0], args[1])
            case "click":
              await self.ensure_selected(args[0] if args else None).click()
            case "wait":
              await sleep(int(args[0]))
            case "wait_random":
              await sleep(random.uniform(int(args[0]), int(args[1])))
            case _:
              log_error(f"Unknown page action: {action}")

        case "element":
          match command:
            case "click":
              await self.ensure_elements()[0].click()
            case "hover":
              await self.ensure_elements()[0].hover()
            case "focus":
              await self.ensure_elements()[0].focus()
            case "press":
              await self.ensure_elements()[0].press(args[0])
            case "select_option":
              await self.ensure_elements()[0].select_option(args[0]) # by value or label
            case "drag_and_drop":
              await self.ensure_elements()[0].drag_and_drop(args[0], args[1]) # src selector, dst selector
            case "upload":
              await self.ensure_elements()[0].set_input_files(args[0])
            case "fill":
              await self.ensure_elements()[0].fill(args[0])
            case "type":
              await self.ensure_elements()[0].press_sequentially(args[0]) # can add delay
            case "check":
              await self.ensure_elements()[0].check()
            case "uncheck":
              await self.ensure_elements()[0].uncheck()
            case "scroll_to":
              await self.ensure_elements()[0].scroll_to(args[0], args[1])
            case _:
              log_error(f"Unknown element action: {action}")

        case "keyboard":
          match command:
            case "press":
              await self.ensure_page().keyboard.press(args[0])
            case "down":
              await self.ensure_page().keyboard.down(args[0])
            case "up":
              await self.ensure_page().keyboard.up(args[0])
            case "type":
              await self.ensure_page().keyboard.type(args[0]) # can add delay
            case _:
              log_error(f"Unknown keyboard action: {action}")

    except Exception as e:
      log_error(f"Error acting {action}: {e}")

async def update_page(page: Page, action: str, selector: str, *args) -> Page:
  """Executes a state-changing action on the page and returns the modified page."""
  try:
    match action:
      case "click":
        await page.click(selector)
      case "goto":
        await page.goto(selector)
      case "hover":
        await page.hover(selector)
      case "focus":
        await page.focus(selector)
      case "press":
        await page.press(selector, args[0])
      case "select_option":
        await page.select_option(selector, args[0])
      case "wait_for_navigation":
        await page.wait_for_navigation()
      case "wait_for_event":
        await page.wait_for_event(selector)
      case "drag_and_drop":
        await page.drag_and_drop(selector, args[0])
      case "upload":
        await page.set_input_files(selector, args[0])
      case "fill":
        await page.fill(selector, args[0])
      case "wait_for_selector":
        await page.wait_for_selector(selector)
      case "wait_for_timeout":
        await page.wait_for_timeout(int(selector))
      case "scroll_to":  # Added scroll action
        await page.evaluate(f'window.scrollTo({selector}, {args[0]})')  
      case "check":  # Added check action
        await page.check(selector)
      case "uncheck": # Added uncheck action
        await page.uncheck(selector)
      case "mouse":
        if args[0] == "move":
          await page.mouse.move(float(selector), float(args[1]))
        elif args[0] == "down":
          await page.mouse.down()
        elif args[0] == "up":
          await page.mouse.up()

      case "set_viewport_size":
        await page.set_viewport_size(ViewportSize(width=int(selector), height=int(args[0])))
      case "emulate":
        await page.emulate(args[0])  # Assuming args[0] contains emulation options (e.g., device name)
      case _:
        log_error(f"Unknown state action: {action}")
  except Exception as e:
    log_error(f"Error executing state action {action}: {e}")

  return page  # Return the modified page

async def schedule(c: Ingester) -> list:
  hashes = {}

  async def ingest(c: Ingester):
    await ensure_claim_task(c)

    hashes.update({f.name: f.target_id for f in c.fields})
    hashes.update({hashes[f.name]: f.name for f in c.fields})

    async with async_playwright() as playwright:
      # futures = action_chains.append(Puppet(hashes[f.name], c, playwright))
      futures = [Puppet.from_field(f, c, playwright) for f in c.fields\
        if Puppet.by_id.get(hashes[f.name]) is None]
      puppets = await gather(*futures) # run all puppets concurrently
      for i, result in enumerate(puppets):
        c.fields[i].value = await puppets[i].ensure_contents()[0]

      await gather(*[p.kill() for p in puppets])
      await transform_and_store(c)

  return [await scheduler.add_ingester(c, fn=ingest, start=False)]
