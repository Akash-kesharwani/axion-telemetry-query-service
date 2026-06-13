import asyncpg
from config import settings
from datetime import datetime, timedelta

pool: asyncpg.Pool | None = None

async def connect_db():
    global pool
    pool = await asyncpg.create_pool(
        dsn=settings.DATABASE_URL,
        min_size=2,
        max_size=10,
    )

async def disconnect_db():
    global pool
    if pool:
        await pool.close()
        pool = None

async def fetch_summary():
    # Return online assets count and last update time
    # "Online" could mean devices seen in the last hour, or simply all distinct devices
    query = """
        SELECT 
            COUNT(DISTINCT device_id) as online_assets,
            MAX(timestamp) as last_update
        FROM telemetry;
    """
    row = await pool.fetchrow(query)
    return {
        "onlineAssets": row["online_assets"] or 0,
        "lastUpdate": row["last_update"]
    }

async def fetch_devices():
    # Get latest status of each device
    query = """
        SELECT DISTINCT ON (device_id)
            device_id,
            device_type,
            refinery_region,
            timestamp as last_seen,
            temperature,
            vibration,
            current
        FROM telemetry
        ORDER BY device_id, timestamp DESC;
    """
    rows = await pool.fetch(query)
    return [dict(r) for r in rows]

async def fetch_latest_telemetry(device_id: str = None):
    if device_id:
        query = """
            SELECT * FROM telemetry
            WHERE device_id = $1
            ORDER BY timestamp DESC
            LIMIT 1;
        """
        row = await pool.fetchrow(query, device_id)
        return dict(row) if row else None
    else:
        query = """
            SELECT * FROM telemetry
            ORDER BY timestamp DESC
            LIMIT 1;
        """
        row = await pool.fetchrow(query)
        return dict(row) if row else None

async def fetch_device_trends(device_id: str, hours: int = 1):
    # Fetch telemetry for the last N hours
    # The timezone is UTC in the DB.
    # We can filter by comparing timestamp to NOW() - INTERVAL 'X hours'
    query = """
        SELECT timestamp, temperature, vibration, current
        FROM telemetry
        WHERE device_id = $1
          AND timestamp >= NOW() - INTERVAL '1 hour' * $2
        ORDER BY timestamp ASC;
    """
    rows = await pool.fetch(query, device_id, hours)
    return [dict(r) for r in rows]

async def fetch_throughput():
    # Query as requested by the user
    query = """
        SELECT
            date_trunc('minute', timestamp) as minute,
            count(*)
        FROM telemetry
        GROUP BY minute
        ORDER BY minute DESC
        LIMIT 60;
    """
    # I added LIMIT 60 and ORDER BY DESC to only show the last hour of throughput, and then I will reverse it so the chart goes left to right
    rows = await pool.fetch(query)
    
    # Reverse to chronological order
    data = [dict(r) for r in rows]
    data.reverse()
    
    # Format the output to match what the user expected: {"minute": "15:10", "count": 120}
    formatted = []
    for r in data:
        formatted.append({
            "minute": r["minute"].strftime("%H:%M") if r["minute"] else "",
            "count": r["count"]
        })
    return formatted
