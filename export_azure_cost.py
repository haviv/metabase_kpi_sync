#!/usr/bin/env python3
"""
Azure subscription cost and tenant metrics via Azure CLI.

Collects:
  - subscription | date | tenant_count | monthly_cost (MTD)
  - subscription | date | service | cost

Tenant count = hyperscale tenant databases across all SQL servers in the subscription
(each DB is one tenant; system DBs like master are excluded).

Can run standalone or from export_ado.py (hourly loop, fetch at most once per 24h).

Env:
  AZURE_COST_SUBSCRIPTIONS  JSON map of env name -> Azure subscription display name
  AZURE_COST_MIN_SYNC_HOURS Minimum hours between fetches (default 24)
  AZURE_COST_SYNC_DAYS      Days of per-service daily costs to refresh (default 3)
"""

import json
import os
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import Column, Date, DateTime, Float, Integer, MetaData, String, Table, text
from sqlalchemy.exc import SQLAlchemyError

env_path = Path(__file__).parent / '.env'
load_dotenv(dotenv_path=env_path)

DEFAULT_SUBSCRIPTIONS = {
    'dev': 'PathlockCloud-Dev',
    'staging': 'PathlockCloud-Staging',
    'pre-prod': 'PathlockCloud-Preprod',
    'prod': 'PathlockCloud-Prod',
}

SYSTEM_DATABASES = {
    'master', 'msdb', 'tempdb', 'model', 'resource',
    'elasticjob', 'datamasking', 'sqlagent', 'sqlserveragent',
}


def ensure_azure_cli_auth():
    """Log in with service principal from env when AZURE_CLIENT_ID/SECRET/TENANT_ID are set."""
    client_id = os.getenv('AZURE_CLIENT_ID', '').strip()
    client_secret = os.getenv('AZURE_CLIENT_SECRET', '').strip()
    tenant_id = os.getenv('AZURE_TENANT_ID', '').strip()
    if not all([client_id, client_secret, tenant_id]):
        return

    result = subprocess.run(
        [
            'az', 'login', '--service-principal',
            '-u', client_id,
            '-p', client_secret,
            '--tenant', tenant_id,
            '--only-show-errors',
        ],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            'Azure service principal login failed: '
            + (result.stderr.strip() or result.stdout.strip())
        )


def _run_az(args, timeout=120):
    ensure_azure_cli_auth()
    result = subprocess.run(
        ['az', *args],
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip() or f'az failed: {args}')
    return result.stdout


def _parse_subscriptions():
    raw = os.getenv('AZURE_COST_SUBSCRIPTIONS', '').strip()
    if raw:
        return json.loads(raw)
    return DEFAULT_SUBSCRIPTIONS


class AzureCostDatabase:
    def __init__(self, engine):
        self.engine = engine
        self.metadata = MetaData()
        self.azure_cost_subscription_daily = Table(
            'azure_cost_subscription_daily', self.metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('subscription', String(50), nullable=False),
            Column('subscription_id', String(64), nullable=False),
            Column('snapshot_date', Date, nullable=False),
            Column('tenant_count', Integer, nullable=False),
            Column('monthly_cost', Float, nullable=False),
            Column('currency', String(10), nullable=False, server_default='USD'),
            Column('created_at', DateTime, nullable=False, server_default=text('CURRENT_TIMESTAMP')),
            Column('updated_at', DateTime, nullable=False, server_default=text('CURRENT_TIMESTAMP')),
        )
        self.azure_cost_service_daily = Table(
            'azure_cost_service_daily', self.metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('subscription', String(50), nullable=False),
            Column('subscription_id', String(64), nullable=False),
            Column('cost_date', Date, nullable=False),
            Column('service_name', String(255), nullable=False),
            Column('cost', Float, nullable=False),
            Column('currency', String(10), nullable=False, server_default='USD'),
            Column('created_at', DateTime, nullable=False, server_default=text('CURRENT_TIMESTAMP')),
            Column('updated_at', DateTime, nullable=False, server_default=text('CURRENT_TIMESTAMP')),
        )
        self.metadata.create_all(self.engine, checkfirst=True)
        with self.engine.connect() as connection:
            connection.execute(text("""
                CREATE UNIQUE INDEX IF NOT EXISTS uq_azure_cost_subscription_daily
                ON azure_cost_subscription_daily (subscription, snapshot_date)
            """))
            connection.execute(text("""
                CREATE UNIQUE INDEX IF NOT EXISTS uq_azure_cost_service_daily
                ON azure_cost_service_daily (subscription, cost_date, service_name)
            """))
            connection.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_azure_cost_subscription_daily_date
                ON azure_cost_subscription_daily (snapshot_date DESC)
            """))
            connection.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_azure_cost_service_daily_date
                ON azure_cost_service_daily (cost_date DESC, subscription)
            """))
            connection.commit()


class AzureCostExtractor:
    def __init__(self, subscriptions=None):
        self.subscriptions = subscriptions or _parse_subscriptions()
        self.sync_days = max(1, int(os.getenv('AZURE_COST_SYNC_DAYS', '3')))
        self._subscription_ids = {}

    def _resolve_subscription_id(self, azure_subscription_name):
        if azure_subscription_name in self._subscription_ids:
            return self._subscription_ids[azure_subscription_name]
        output = _run_az([
            'account', 'list',
            '--query', f"[?name=='{azure_subscription_name}'].id | [0]",
            '-o', 'tsv',
        ])
        subscription_id = output.strip()
        if not subscription_id:
            raise RuntimeError(f'Azure subscription not found: {azure_subscription_name}')
        self._subscription_ids[azure_subscription_name] = subscription_id
        return subscription_id

    def _is_tenant_database(self, database):
        name = (database.get('name') or '').lower()
        if name in SYSTEM_DATABASES:
            return False
        sku = database.get('currentSku') or {}
        tier = (sku.get('tier') or '').lower()
        sku_name = (sku.get('name') or '').lower()
        return tier == 'hyperscale' or sku_name == 'elasticpool'

    def count_tenant_databases(self, azure_subscription_name):
        servers = json.loads(_run_az([
            'sql', 'server', 'list',
            '--subscription', azure_subscription_name,
            '-o', 'json',
        ], timeout=180))
        tenant_count = 0
        for server in servers:
            databases = json.loads(_run_az([
                'sql', 'db', 'list',
                '--server', server['name'],
                '--resource-group', server['resourceGroup'],
                '--subscription', azure_subscription_name,
                '-o', 'json',
            ], timeout=180))
            tenant_count += sum(1 for db in databases if self._is_tenant_database(db))
        return tenant_count

    def _cost_query(self, subscription_id, body):
        output = _run_az([
            'rest', '--method', 'post',
            '--url',
            f'https://management.azure.com/subscriptions/{subscription_id}/providers/Microsoft.CostManagement/query?api-version=2023-11-01',
            '--body', json.dumps(body),
        ], timeout=180)
        return json.loads(output)

    def get_month_to_date_cost(self, subscription_id):
        payload = self._cost_query(subscription_id, {
            'type': 'ActualCost',
            'timeframe': 'MonthToDate',
            'dataset': {
                'granularity': 'None',
                'aggregation': {
                    'totalCost': {'name': 'Cost', 'function': 'Sum'},
                },
            },
        })
        rows = payload.get('properties', {}).get('rows', [])
        columns = [col['name'] for col in payload.get('properties', {}).get('columns', [])]
        if not rows:
            return 0.0, 'USD'
        row = rows[0]
        cost_idx = columns.index('Cost') if 'Cost' in columns else 0
        currency_idx = columns.index('Currency') if 'Currency' in columns else None
        cost = float(row[cost_idx] or 0)
        currency = row[currency_idx] if currency_idx is not None else 'USD'
        return cost, currency

    def get_daily_service_costs(self, subscription_id, start_date, end_date):
        payload = self._cost_query(subscription_id, {
            'type': 'ActualCost',
            'timeframe': 'Custom',
            'timePeriod': {
                'from': f'{start_date.isoformat()}T00:00:00Z',
                'to': f'{end_date.isoformat()}T23:59:59Z',
            },
            'dataset': {
                'granularity': 'Daily',
                'aggregation': {
                    'totalCost': {'name': 'Cost', 'function': 'Sum'},
                },
                'grouping': [
                    {'type': 'Dimension', 'name': 'ServiceName'},
                ],
            },
        })
        rows = payload.get('properties', {}).get('rows', [])
        columns = [col['name'] for col in payload.get('properties', {}).get('columns', [])]
        cost_idx = columns.index('Cost')
        date_idx = columns.index('UsageDate')
        service_idx = columns.index('ServiceName')
        currency_idx = columns.index('Currency') if 'Currency' in columns else None

        results = []
        for row in rows:
            usage_date = str(row[date_idx])
            cost_date = datetime.strptime(usage_date, '%Y%m%d').date()
            results.append({
                'cost_date': cost_date,
                'service_name': row[service_idx] or 'Unknown',
                'cost': float(row[cost_idx] or 0),
                'currency': row[currency_idx] if currency_idx is not None else 'USD',
            })
        return results

    def sync_to_database(self, db_connection):
        cost_db = AzureCostDatabase(db_connection.engine)
        snapshot_date = datetime.now().date()
        service_start = snapshot_date - timedelta(days=self.sync_days - 1)
        processed = 0

        with db_connection.engine.connect() as connection:
            try:
                for subscription, azure_name in self.subscriptions.items():
                    print(f"------>Azure cost sync for {subscription} ({azure_name})")
                    subscription_id = self._resolve_subscription_id(azure_name)

                    tenant_count = self.count_tenant_databases(azure_name)
                    monthly_cost, currency = self.get_month_to_date_cost(subscription_id)
                    print(
                        f"        tenants={tenant_count}, month_to_date_cost={monthly_cost:.2f} {currency}"
                    )

                    connection.execute(
                        text("""
                            INSERT INTO azure_cost_subscription_daily
                                (subscription, subscription_id, snapshot_date, tenant_count, monthly_cost, currency, updated_at)
                            VALUES
                                (:subscription, :subscription_id, :snapshot_date, :tenant_count, :monthly_cost, :currency, CURRENT_TIMESTAMP)
                            ON CONFLICT (subscription, snapshot_date)
                            DO UPDATE SET
                                subscription_id = EXCLUDED.subscription_id,
                                tenant_count = EXCLUDED.tenant_count,
                                monthly_cost = EXCLUDED.monthly_cost,
                                currency = EXCLUDED.currency,
                                updated_at = CURRENT_TIMESTAMP
                        """),
                        {
                            'subscription': subscription,
                            'subscription_id': subscription_id,
                            'snapshot_date': snapshot_date,
                            'tenant_count': tenant_count,
                            'monthly_cost': monthly_cost,
                            'currency': currency,
                        },
                    )
                    processed += 1

                    service_rows = self.get_daily_service_costs(subscription_id, service_start, snapshot_date)
                    print(f"        service cost rows={len(service_rows)} ({service_start} to {snapshot_date})")
                    for row in service_rows:
                        connection.execute(
                            text("""
                                INSERT INTO azure_cost_service_daily
                                    (subscription, subscription_id, cost_date, service_name, cost, currency, updated_at)
                                VALUES
                                    (:subscription, :subscription_id, :cost_date, :service_name, :cost, :currency, CURRENT_TIMESTAMP)
                                ON CONFLICT (subscription, cost_date, service_name)
                                DO UPDATE SET
                                    subscription_id = EXCLUDED.subscription_id,
                                    cost = EXCLUDED.cost,
                                    currency = EXCLUDED.currency,
                                    updated_at = CURRENT_TIMESTAMP
                            """),
                            {
                                'subscription': subscription,
                                'subscription_id': subscription_id,
                                'cost_date': row['cost_date'],
                                'service_name': row['service_name'],
                                'cost': row['cost'],
                                'currency': row['currency'],
                            },
                        )
                    processed += len(service_rows)

                connection.commit()
            except SQLAlchemyError as e:
                connection.rollback()
                raise RuntimeError(f'Database error during Azure cost sync: {e}') from e

        return processed


def should_sync_azure_cost(last_sync_time, min_hours=24):
    if not last_sync_time or last_sync_time == datetime(2025, 3, 1):
        return True
    return datetime.now() - last_sync_time >= timedelta(hours=min_hours)


if __name__ == '__main__':
    from export_ado import get_database_connection

    extractor = AzureCostExtractor()
    db = get_database_connection()
    count = extractor.sync_to_database(db)
    print(f'Processed {count} Azure cost records')
