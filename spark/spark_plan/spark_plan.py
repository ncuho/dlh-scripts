"""Reviewed Spark SQL for the prepared source case. No local Spark or credentials."""
from pathlib import Path
import re
ORDER = [
    'hub_field',
    'hub_well',
    'hub_production_day',
    'hub_downtime',
    'link_well_field',
    'link_production_day_well',
    'link_downtime_well',
    'sat_field',
    'sat_well',
    'sat_production_day',
    'sat_downtime',
    'dim_field',
    'dim_well',
    'fact_well_day',
    'agg_field_day',
    'dim_downtime_reason',
    'fact_downtime_interval',
    'agg_field_downtime_day',
    'field_performance',
]
EXTRA={'hub_downtime','link_downtime_well','sat_downtime','dim_downtime_reason','fact_downtime_interval','agg_field_downtime_day','field_performance'}
def plan(schema,chain='performance'):
    if not re.fullmatch(r'oil_r_[a-z0-9_]{1,42}',schema):raise ValueError('invalid owned release')
    if chain not in {'production','performance'}:raise ValueError('unknown business chain')
    root=Path(__file__).resolve().parents[1]/'sql'
    return [(name,(root/name/(name+'.sql')).read_text().replace('${schema}',schema)) for name in ORDER if chain=='performance' or name not in EXTRA]
