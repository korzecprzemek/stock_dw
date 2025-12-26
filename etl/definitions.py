# etl/definitions.py

import dagster as dg

from dagster import Definitions
from etl.resources import db_resource
from etl.jobs.daily_price_job import daily_price_job
from etl.jobs.intraday_job import intraday_job
# from etl.resources import db_resource

# ---------- SCHEDULE: dzienny batch EOD ----------
defs = Definitions(
    jobs=[daily_price_job, intraday_job],
)

daily_price_schedule = dg.ScheduleDefinition(
    name="daily_price_schedule",
    job=daily_price_job,
    # 23:30, pon–pt (min godz dzien mies dzien_tyg)
    cron_schedule="30 23 * * 1-5",
    execution_timezone="Europe/Warsaw",
)

# ---------- SCHEDULE: intraday micro-batch ----------

intraday_schedule = dg.ScheduleDefinition(
    name="intraday_schedule",
    job=intraday_job,
    # co 5 minut między 15:30 a 22:30, pon–pt
    cron_schedule="*/5 15-22 * * 1-5",
    execution_timezone="Europe/Warsaw",
)

# ---------- DEFINITIONS ----------

defs = dg.Definitions(
    jobs=[daily_price_job, intraday_job],
    schedules=[daily_price_schedule, intraday_schedule],
    # resources={"db": db_resource},
)
