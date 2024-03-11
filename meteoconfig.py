from dateutil import tz


# Timers
timer = 30  # seconds
error_timer = 120  # seconds
kill_timer = 180  # seconds
wait_for_kill = 20  # seconds

# InfluxDB settings
influxdb_url = 'http://localhost:8086'
influxdb_token = 'ABCD123456789QWERTZUIOP'
influxdb_org = 'org'
influxdb_bucket = 'bucket'
influxdb_measurement = 'winmeteo'

# DBF file path
dbf_path = 'C:/WinMeteo/Data/meteo.DBF'

# Timezone
local_zone = tz.gettz('Europe/Prague')

# WinMeteo settings
winmeteo_process = 'WinMeteo.exe'
winmeteo_path = 'C:/WinMeteo/WinMeteo.exe'
