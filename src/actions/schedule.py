from asyncio import gather, get_running_loop, Task
from aiocron import Cron, crontab

from src.cache import claim_task, ensure_claim_task, is_task_claimed
from src.utils import log_debug, log_info, log_warn, log_error, submit_to_threadpool,\
  Interval, interval_to_cron
from src.model import Ingester, IngesterType
import src.state as state

def get_scheduler(ingestor_type: IngesterType) -> callable:
  import src.ingesters as ingesters # runtime circular import (could use pacage name reflection)
  # mapping of available ingesters to their respective functions
  SCHEDULER_BY_TYPE: dict[IngesterType, callable] = {
    "scrapper": ingesters.static_scrapper.schedule,
    "http_api": ingesters.http_api.schedule,
    "ws_api": ingesters.ws_api.schedule,
    "evm_caller": ingesters.evm_caller.schedule,
    "evm_logger": ingesters.evm_logger.schedule,
    # "fix_api": ingesters.fix_api.schedule,
  }
  return SCHEDULER_BY_TYPE.get(ingestor_type, None)

async def monitor_cron(cron: Cron):
  while True:
    try:
      await cron.next()
    except Exception as e:
      log_error(f"Cron job failed with exception: {e}")
      get_running_loop().stop()
      break

async def check_ingesters_integrity(ingesters: list[Ingester]):
  log_debug("TODO: implement ingesters integrity check (eg. if claimed resource, check last ingestion time+tsdb table schema vs resource schema...)")

class Scheduler:
  def __init__(self):
    self.cron_by_job_id = {}
    self.cron_by_interval = {}
    self.jobs_by_interval = {}
    self.job_by_id = {}

  def run_threaded(self, job_ids: list[str]):
    jobs = [self.job_by_id[j] for j in job_ids]
    ft = [submit_to_threadpool(state.thread_pool, job[0], *job[1]) for job in jobs]
    return [f.result() for f in ft] # wait for all results

  async def run_async(self, job_ids: list[str]):
    jobs = [self.job_by_id[j] for j in job_ids]
    ft = [job[0](*job[1]) for job in jobs]
    return await gather(*ft)

  async def add(self, id: str, fn: callable, args: list, interval: Interval="h1", start=True, threaded=False) -> Task:
    if id in self.job_by_id:
      raise ValueError(f"Duplicate job id: {id}")
    self.job_by_id[id] = (fn, args)

    jobs = self.jobs_by_interval.setdefault(interval, [])
    jobs.append(id)

    if not start:
      return None

    return start(self, interval, threaded)

  async def start_interval(self, interval: Interval, threaded=False) -> Task:
    if interval in self.cron_by_interval:
      old_cron = self.cron_by_interval[interval]
      old_cron.stop() # stop prev cron

    job_ids = self.jobs_by_interval[interval]

    # cron = crontab(interval_to_cron(interval), func=self.run_threaded if threaded else self.run_async, args=(job_ids,))
    cron = crontab(interval_to_cron(interval), func=self.run_async, args=(job_ids,))

    # update the interval's jobs cron ref
    for id in job_ids:
      self.cron_by_job_id[id] = cron
    self.cron_by_interval[interval] = cron

    log_info(f"Proc {state.args.proc_id} starting {interval} {'threaded' if threaded else 'async'} cron with {len(job_ids)} jobs: {job_ids}")
    return await monitor_cron(self.cron_by_job_id[id])

  async def start(self, threaded=False) -> list[Task]:
    intervals, jobs = self.jobs_by_interval.keys(), self.job_by_id.keys()
    log_info(f"Starting {len(jobs)} jobs ({len(intervals)} crons: {list(intervals)})")
    return [self.start_interval(i, threaded) for i in self.jobs_by_interval.keys()]

  async def add_ingester(self, c: Ingester, fn: callable, start=True, threaded=False) -> Task:
    return await self.add(id=c.id, fn=fn, args=(c,), interval=c.interval, start=start, threaded=threaded)

  async def add_ingesters(self, ingesters: list[Ingester], fn: callable, start=True, threaded=False) -> list[Task]:
    intervals = set([c.interval for c in ingesters])
    added = await gather(*[self.add_ingester(c, fn, start=False, threaded=threaded) for c in ingesters])
    if start:
      await gather(*[self.start_interval(i, threaded) for i in intervals])

async def schedule(c: Ingester) -> list[Task]:
  schedule = get_scheduler(c.ingester_type)
  if not schedule:
    raise ValueError(f"Unsupported ingester type: {c.type}")
  await ensure_claim_task(c)
  tasks = await schedule(c)
  log_debug(f"Scheduled for ingestion: {c.name}.{c.interval} [{', '.join([field.name for field in c.fields])}]")
  return tasks

scheduler = Scheduler()
