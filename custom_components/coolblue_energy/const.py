from datetime import timedelta

from homeassistant.const import CURRENCY_EURO, UnitOfEnergy, UnitOfVolume

DOMAIN = "coolblue_energy"
DEFAULT_NAME = "Coolblue Energy"

PLATFORMS = ["sensor"]
SCAN_INTERVAL = timedelta(hours=6)
BACKFILL_DAYS = 7
# How many recent days to re-check on every normal refresh.
# Coolblue sometimes publishes data hours late, so we look back this many days
# to catch any days that were empty on the previous poll.
RETRY_DAYS = 3

# Config-entry data keys
CONF_DEBTOR_ID = "debtor_id"
CONF_LOCATION_ID = "location_id"

# Service names
SERVICE_REIMPORT_STATISTICS = "reimport_statistics"
ATTR_START_DATE = "start_date"

# External statistic IDs — must be prefixed with DOMAIN
STAT_ELECTRICITY_CONSUMED = f"{DOMAIN}:electricity_consumed"
STAT_ELECTRICITY_RETURNED = f"{DOMAIN}:electricity_returned"
STAT_GAS_CONSUMED = f"{DOMAIN}:gas_consumed"

EUR_KWH = f"{CURRENCY_EURO}/{UnitOfEnergy.KILO_WATT_HOUR}"
EUR_M3 = f"{CURRENCY_EURO}/{UnitOfVolume.CUBIC_METERS}"
