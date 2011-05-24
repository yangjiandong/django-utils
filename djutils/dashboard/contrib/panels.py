from django.conf import settings

from djutils.dashboard.provider import PanelProvider
from djutils.dashboard.registry import registry

try:
    import psycopg2
except ImportError:
    psycopg2 = None

def get_db_setting(key):
    try:
        return settings.DATABASES['default'][key]
    except KeyError:
        return getattr(settings, 'DATABASE_%s' % key)


class PostgresPanelProvider(PanelProvider):
    conn = None
    
    def connect(self):
        self.conn = self.get_conn()
    
    def get_conn(self):
        return psycopg2.connect(
            database=get_db_setting('NAME'),
            user=get_db_setting('USER') or 'postgres'
        )
    
    def execute(self, sql, params=None):
        if not self.conn:
            self.connect()
        
        cursor = self.conn.cursor()
        res = cursor.execute(sql, params or ())
        return cursor


class PostgresQueryPanel(PostgresPanelProvider):
    def get_title(self):
        return 'Postgres Queries'

    def get_data(self):
        cursor = self.execute('SELECT datname, current_query, query_start FROM pg_stat_activity ORDER BY query_start;')
        rows = cursor.fetchall()
        
        idle = idle_trans = queries = 0
        
        for (database, query, start) in rows:
            if query == '<IDLE>':
                idle += 1
            elif query == '<IDLE> in transaction':
                idle_trans += 1
            else:
                queries += 1
        
        return {
            'idle': idle,
            'idle_in_trans': idle_trans,
            'queries': queries
        }


class PostgresUserPanel(PostgresPanelProvider):
    def get_title(self):
        return 'Postgres Connections by User'
    
    def get_data(self):
        cursor = self.execute('SELECT usename, count(*) FROM pg_stat_activity WHERE procpid != pg_backend_pid() GROUP BY usename ORDER BY 1;')
        
        data_dict = {}
        
        for row in cursor.fetchall():
            data_dict[row[0]] = row[1]
        
        return data_dict


class PostgresConnectionsPanel(PostgresPanelProvider):
    def get_title(self):
        return 'Postgres connections'
    
    def get_data(self):
        sql = """
            SELECT tmp.state, COALESCE(count, 0) FROM
                (VALUES ('active'), ('waiting'), ('idle'), ('idletransaction'), ('unknown')) AS tmp(state)
            LEFT JOIN (
                SELECT CASE
                    WHEN waiting THEN 'waiting'
                    WHEN current_query='<IDLE>' THEN 'idle' 
                    WHEN current_query='<IDLE> in transaction' THEN 'idletransaction' 
                    WHEN current_query='<insufficient privilege>' THEN 'unknown' 
                    ELSE 'active' END AS state,
                count(*) AS count
                FROM pg_stat_activity WHERE procpid != pg_backend_pid()
                GROUP BY CASE WHEN waiting THEN 'waiting' WHEN current_query='<IDLE>' THEN 'idle' WHEN current_query='<IDLE> in transaction' THEN 'idletransaction' WHEN current_query='<insufficient privilege>' THEN 'unknown' ELSE 'active' END
            ) AS tmp2
            ON tmp.state=tmp2.state
            ORDER BY 1
        """
        cursor = self.execute(sql)
        
        data_dict = {}
        total = 0
        
        for row in cursor.fetchall():
            data_dict[row[0]] = row[1]
            total += row[1]
        
        data_dict['total'] = total
        
        return data_dict


class PostgresConnectionsForDatabase(PostgresPanelProvider):
    def get_title(self):
        return 'Database connections'
    
    def get_data(self):
        sql = """
            SELECT pg_database.datname, COALESCE(count,0) AS count 
            FROM pg_database 
            LEFT JOIN (
                SELECT datname, count(*)
                FROM pg_stat_activity 
                WHERE procpid != pg_backend_pid()
                GROUP BY datname
            ) AS tmp ON pg_database.datname=tmp.datname 
            WHERE datallowconn AND pg_database.datname=%s ORDER BY 1
        """
        cursor = self.execute(sql, (get_db_setting('NAME'),))
        
        data_dict = {}
        
        for row in cursor.fetchall():
            data_dict[row[0]] = row[1]
        
        return data_dict


if 'psycopg2' in get_db_setting('ENGINE'):
    registry.register(PostgresQueryPanel)
    registry.register(PostgresUserPanel)
    registry.register(PostgresConnectionsPanel)
    registry.register(PostgresConnectionsForDatabase)
