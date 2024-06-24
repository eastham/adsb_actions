"""Parsing and representation for a single position update coming from ADS-B."""

from dataclasses import dataclass, fields
from typing import Optional
from icao_convert import icao_to_n_or_c

from geopy import distance

import adsb_logger
from adsb_logger import Logger

logger = adsb_logger.logging.getLogger(__name__)
#logger.level = adsb_logger.logging.DEBUG
LOGGER = Logger()

@dataclass
class Location:
    """A single aircraft position + data update """
    lat: float = 0.
    lon: float = 0.
    alt_baro: int = 0
    now: Optional[float] = 0
    flight: Optional[str] = "N/A" # the flight id
    hex: Optional[str] = None   # ICAO code
    tail: Optional[str] = None  # tail number from ICAO code
    gs: Optional[float] = 0
    track: float = 0.
    flightdict: Optional[dict] = None

    def __post_init__(self):
        """sometimes these values come in as strings when not available"""
        if not isinstance(self.lat, float): self.lat = 0
        if not isinstance(self.lon, float): self.lon = 0
        if not isinstance(self.alt_baro, int): self.alt_baro = 0
        if not isinstance(self.gs, float): self.gs = 0
        if not isinstance(self.track, float): self.track = 0.

    @classmethod
    def from_dict(cl, d: dict):
        """Return a location created from a dict bearing the same-named fields."""
        nd = {}
        for f in fields(Location):
            if f.name in d:
                nd[f.name] = d[f.name]

        # XXX should this be in flight?
        if "hex" in d:
            tail = icao_to_n_or_c(d["hex"])
            if tail:
                nd["tail"] = tail
        return Location(**nd)

    def to_str(self):
        s = "%s/%s: %d MSL %d deg %.1f kts %.4f, %.4f" % (self.tail,
            self.flight, self.alt_baro,
            self.track, self.gs, self.lat, self.lon)
        return s

    def __sub__(self, other):
        """Return distance to the other Location in nm"""
        return self.distfrom(other.lat, other.lon)

    def __lt__(self, other):
        """Altitude comparison only"""
        return self.alt_baro < other.alt_baro

    def __gt__(self, other):
        """Altitude comparison only"""
        return self.alt_baro > other.alt_baro

    def distfrom(self, lat, lon):
        """Return distance from other lat/long in nm"""
        return distance.distance((self.lat, self.lon), (lat, lon)).nm
