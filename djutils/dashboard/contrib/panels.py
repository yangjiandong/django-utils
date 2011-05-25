import socket

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


# stored as either 'localhost:6379' or 'localhost:6379:0'
REDIS_SERVER = getattr(settings, 'DASHBOARD_REDIS_CONNECTION', None)

# stored as 'localhost:11211'
MEMCACHED_SERVER = getattr(settings, 'DASHBOARD_MEMCACHED_CONNECTION', None)


class PostgresPanelProvider(PanelProvider):
    conn = None
    
    def connect(self):
        self.conn = self.get_conn()
    
    def get_conn(self):
        return psycopg2.connect(
            database=get_db_setting('NAME'),
            user=get_db_setting('USER') or 'postgres',
            host=get_db_setting('HOST') or 'localhost',
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
        return 'Postgres connections by User'
    
    def get_data(self):
        cursor = self.execute('SELECT usename, count(*) FROM pg_stat_activity WHERE procpid != pg_backend_pid() GROUP BY usename ORDER BY 1;')
        
        data_dict = {}
        
        for row in cursor.fetchall():
            data_dict[row[0]] = row[1]
        
        return data_dict


class PostgresConnectionsPanel(PostgresPanelProvider):
    def get_title(self):
        return 'Postgres connections by Type'
    
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
        return 'Connections for site db'
    
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


class RedisPanelProvider(PanelProvider):
    def get_info(self):
        host, port = REDIS_SERVER.split(':')[:2]
        sock = socket.socket()
        try:
            sock.connect((host, int(port)))
        except:
            return {}
        
        sock.send('INFO\r\n')
        data = sock.recv(4096)
        data_dict = {}
        for line in data.splitlines():
            if ':' in line:
                key, val = line.split(':', 1)
                data_dict[key] = val
        
        return data_dict
    
    def get_key(self, key):
        return self.get_info().get(key, 0)


class RedisConnectedClients(RedisPanelProvider):
    def get_title(self):
        return 'Redis connections'
    
    def get_data(self):
        return {'clients': self.get_key('connected_clients')}


class RedisMemoryUsage(RedisPanelProvider):
    def get_title(self):
        return 'Redis memory usage'
    
    def get_data(self):
        return {'memory': self.get_key('used_memory')}


class CPUInfo(PanelProvider):
    def get_title(self):
        return 'CPU Usage'
    
    def get_data(self):
        fh = open('/proc/loadavg', 'r')
        contents = fh.read()
        fh.close()
        
        # grab the second value
        second = contents.split()[1]
        
        return {'loadavg': second}


class MemcachedPanelProvider(PanelProvider):
    def get_stats(self):
        host, port = MEMCACHED_SERVER.split(':')[:2]
        sock = socket.socket()
        try:
            sock.connect((host, int(port)))
        except:
            return {}
        
        sock.send('stats\r\n')
        data = sock.recv(8192)
        data_dict = {}
        for line in data.splitlines():
            if line.startswith('STAT'):
                key, val = line.split()[1:]
                data_dict[key] = val
        
        return data_dict


class MemcachedHitMiss(MemcachedPanelProvider):
    def get_title(self):
        return 'Memcached hit/miss ratio'
    
    def get_data(self):
        memcached_stats = self.get_stats()
        
        get_hits = float(memcached_stats.get('get_hits', 0))
        get_misses = float(memcached_stats.get('get_misses', 0))
        
        if get_hits and get_misses:
            return {'hit-miss ratio': get_hits / get_misses}
        
        return {'hit-miss-ration': 0}


class MemcachedMemoryUsage(MemcachedPanelProvider):
    def get_title(self):
        return 'Memcached memory usage'
    
    def get_data(self):
        memcached_stats = self.get_stats()
        
        return {'bytes': memcached_stats.get('bytes', 0)}


class MemcachedItemsInCache(MemcachedPanelProvider):
    def get_title(self):
        return 'Memcached items in cache'
    
    def get_data(self):
        memcached_stats = self.get_stats()
        return {'items': memcached_stats.get('curr_items', 0)}


registry.register(CPUInfo)


if REDIS_SERVER:
    registry.register(RedisConnectedClients)
    registry.register(RedisMemoryUsage)


if MEMCACHED_SERVER:
    registry.register(MemcachedHitMiss)
    registry.register(MemcachedMemoryUsage)
    registry.register(MemcachedItemsInCache)


if 'psycopg2' in get_db_setting('ENGINE'):
    registry.register(PostgresQueryPanel)
    registry.register(PostgresUserPanel)
    registry.register(PostgresConnectionsPanel)
    registry.register(PostgresConnectionsForDatabase)
