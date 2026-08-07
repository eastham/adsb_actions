import os
import logging
import threading
import time
from applications.airport_monitor.los import process_los_launch, LOS
from applications.airport_monitor.db_ops import add_op
from core.database.interface import get_database
from adsb_actions.stats import Stats
from prometheus_client import Gauge

from adsb_actions.adsb_logger import Logger
import pygame

logger = logging.getLogger(__name__)
#logger.level = logging.DEBUG
LOGGER = Logger()

Stats.register_prom_callbacks()

def landing_cb(flight):
    logger.info("Landing detected! %s", flight.flight_id)
    if 'note' in flight.flags:
        logger.info("Local-flight landing detected! %s", flight.flight_id)
        Stats.local_landings += 1
    Stats.landings += 1

    add_op(flight, "Landing", 'note' in flight.flags)

def popup_takeoff_cb(flight):
    logger.info("Popup takeoff detected! %s", flight.flight_id)
    Stats.popup_takeoffs += 1
    takeoff_cb(flight)

def takeoff_cb(flight):
    logger.info("Takeoff detected! %s", flight.flight_id)
    Stats.takeoffs += 1

    add_op(flight, "Takeoff", False)

def los_cb(flight1, flight2):
    """LOS = Loss of Separation -- two airplanes in close proximity"""
    logger.info("LOS detected! %s", flight1.flight_id)
    launch_alert_audio(False, f"LOS detected {flight1.flight_id}")
    process_los_launch(flight1, flight2)

# Guards audio playback: playsound blocks for the length of the clip, so
# playback runs off-thread, and overlapping triggers are dropped rather than
# queued -- a stale alert is worse than a missed repeat.  Both LOS and
# vehicle-on-runway fire repeatedly while the event is underway, so a shared
# cooldown keeps a single event from replaying the alert on a loop.
AUDIO_COOLDOWN_SECS = 30

_audio_lock = threading.Lock()
_audio_playing = False
# monotonic time the last clip finished.  None means "never played" -- can't
# use 0.0, since monotonic() is boot-relative and would put a freshly-booted
# machine inside the cooldown for its first alert.
_audio_last_finished = None

TONE_FILE = "./src/sounds/airbus-master-warning-sound-high-quality.mp3"
VEHICLE_ON_RUNWAY_FILE = "./src/sounds/hesteah_caution.mp3"

def _play_clip(path):
    """Play one clip and wait for it to finish.  pygame's play() returns
    immediately, so we poll the channel to keep clips sequential."""
    channel = pygame.mixer.Sound(path).play()
    while channel.get_busy():
        time.sleep(0.05)

def _play_alert_audio(vehicle_on_runway):
    """Play the warning tone, optionally followed by the vehicle-on-runway
    voice callout.  Blocks for the length of the clips -- run via
    launch_alert_audio(), not directly from a callback."""
    global _audio_playing, _audio_last_finished

    try:
        # mixer init is deferred to first use: it grabs the audio device, and
        # importing this module must not do that (offline analysis imports it).
        if not pygame.mixer.get_init():
            pygame.mixer.init()

        _play_clip(TONE_FILE)
        if vehicle_on_runway:
            _play_clip(VEHICLE_ON_RUNWAY_FILE)
    except Exception:      # keep a bad/missing sound file from killing the thread
        logger.exception("Alert audio playback failed")
    finally:
        # cooldown starts when playback ends, not when it began, so clip
        # length doesn't eat into the quiet period.
        with _audio_lock:
            _audio_last_finished = time.monotonic()
            _audio_playing = False

def launch_alert_audio(vehicle_on_runway, context):
    """Start alert audio in the background, unless a clip is already playing
    or we're still inside the cooldown.  context is just for logging."""
    global _audio_playing

    with _audio_lock:
        if _audio_playing:
            logger.info("%s: audio already playing, skipping...", context)
            return
        quiet_for = (time.monotonic() - _audio_last_finished
                     if _audio_last_finished is not None else None)
        if quiet_for is not None and quiet_for < AUDIO_COOLDOWN_SECS:
            logger.info("%s: in %ds audio cooldown (%ds remaining), skipping...",
                        context, AUDIO_COOLDOWN_SECS,
                        round(AUDIO_COOLDOWN_SECS - quiet_for))
            return
        _audio_playing = True

    logger.info("%s: playing audio...", context)
    threading.Thread(target=_play_alert_audio, args=[vehicle_on_runway],
                     daemon=True).start()

def play_vehicle_on_runway_audio(flight):
    launch_alert_audio(True, f"Vehicle on runway detected {flight.flight_id}")

def register_callbacks(adsb_actions):
    adsb_actions.register_callback("landing", landing_cb)
    adsb_actions.register_callback("takeoff", takeoff_cb)
    adsb_actions.register_callback("popup_takeoff", popup_takeoff_cb)
    adsb_actions.register_callback("los_update_cb", los_cb)
    adsb_actions.register_callback("vehicle_on_runway_audio_cb",
                                    play_vehicle_on_runway_audio)

def enter_db_fake_mode():
    get_database().enter_fake_mode()

def exit_workers():
    LOS.quit = True
    logger.info("Please wait for final LOS GC...")
