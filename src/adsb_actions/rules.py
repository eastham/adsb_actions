"""This module parses rules and actions, and applies them to flight data."""

import datetime
import logging
import shlex
import subprocess
import sys
from typing import Callable
from .flight import Flight
from .stats import Stats
from .ruleexecutionlog import RuleExecutionLog, ExecutionCounter
from .adsb_logger import Logger
from .webhooks import send_webhook

logger = logging.getLogger(__name__)
logger.level = logging.INFO
LOGGER = Logger()

class Rules:
    """
    This class represents the conditions, agctions, and associated state from one
    yaml rule file.

    Attributes:
        yaml_data (dict): A dictionary containing the data from the YAML file.
        rule_exection_log (RuleExecutionLog): Tracks when rules were last fired,
            to support the "cooldown" condition.
        callbacks (dict[str, Callable]): callbacks that were registered with
            AdsbActions.register_callback().  Mapping from callback name to 
            function.
    """

    def __init__(self, data):
        self.yaml_data : dict = data
        self.rule_execution_log = RuleExecutionLog()
        self.callbacks : dict[str, Callable]= {}    # mapping from yaml name to fn

        if self.get_rules() is {}:
            logger.warning("No rules found in YAML")
            return

        # YAML rules correctness checks
        for rule in self.get_rules().values():
            assert self.conditions_valid(rule['conditions']), "Invalid conditions, see log for more info"
            assert self.actions_valid(rule['actions']), "Invalid actions, see log for more info"

    def get_rules(self) -> list:
        if not 'rules' in self.yaml_data:
            return {}
        else:
            return self.yaml_data['rules']

    def process_flight(self, flight: Flight) -> None:
        """Apply rules and actions to the current position of a given flight. """

        rule_items = self.get_rules().items()

        for rule_name, rule_value in rule_items:
            logger.debug("Checking rules %s", rule_name)

            if self.conditions_match(flight, rule_value['conditions'], rule_name):
                logger.debug("MATCH for rule '%s' for flight %s", rule_name, flight.flight_id)

                self.do_actions(flight, rule_value['actions'], rule_name)
            else:
                logger.debug("NOMATCH for rule '%s' for flight %s", rule_name, flight.flight_id)

    def conditions_valid(self, conditions: dict):
        """Check for invalid or unknown conditions, return True if valid."""
        VALID_CONDITIONS = ['proximity', 'aircraft_list', 'exclude_aircraft_list',
                            'exclude_aircraft_substrs',
                            'min_alt', 'max_alt',
                            'transition_regions', 'changed_regions',
                            'regions', 'latlongring',
                            'cooldown', 'rule_cooldown', 'has_attr', 'min_time',
                            'max_time', 'time_ranges', 'enabled', 'squawk',
                            'emergency', 'category', 'min_gs', 'max_gs',
                            'min_vertical_rate', 'max_vertical_rate',
                            'callsign_prefix', 'on_ground']

        try:
            for condition in conditions.keys():
                if condition not in VALID_CONDITIONS:
                    logger.error("Unknown condition: %s", condition)
                    return False
        except AttributeError:
            logger.error("Specify unconditional execution with '{}' in YAML")
            raise
        return True

    def conditions_match(self, flight: Flight, conditions: dict,
                         rule_name: str) -> bool:
        """Determine if the given rule conditions match for the given
        flight.  Returns true on match for the specific rule given, false
        otherwise.
        Note: Put expensive-to-evaluate conditions toward the bottom,
        for best performance."""

        #logger.debug("condition_match checking rules: %s", str(conditions))
        Stats.condition_match_calls += 1

        # Check enabled condition first - cheap and can short-circuit evaluation
        if 'enabled' in conditions:
            if not conditions['enabled']:
                return False

        if 'proximity' in conditions:
            return False  # handled asynchronously in handle_proximity_conditions

        if 'aircraft_list' in conditions:
            condition_value = conditions['aircraft_list']
            try:
                ac_list = self.yaml_data['aircraft_lists'][condition_value]
            except KeyError:
                logger.critical("Aircraft list not found: %s", condition_value)
                return False
            result = flight.flight_id in ac_list
            if not result:
                return False

        if 'exclude_aircraft_list' in conditions:
            condition_value = conditions['exclude_aircraft_list']
            try:
                ac_list = self.yaml_data['aircraft_lists'][condition_value]
            except KeyError:
                logger.critical("Aircraft list not found: %s", condition_value)
                return False
            result = flight.flight_id not in ac_list
            if not result:
                return False

        if 'exclude_aircraft_substrs' in conditions:
            condition_value = conditions['exclude_aircraft_substrs']
            for value in condition_value:
                result = value in flight.flight_id
                if result:
                    return False

        if 'min_alt' in conditions:
            condition_value = conditions['min_alt']
            result = flight.lastloc.alt_baro >= int(condition_value)
            if not result:
                return False

        if 'max_alt' in conditions:
            condition_value = conditions['max_alt']
            result = flight.lastloc.alt_baro <= int(condition_value)
            if not result:
                return False

        if 'transition_regions' in conditions:
            # moved from one region to another.  None is a valid region.
            condition_value = conditions['transition_regions']
            result = (flight.was_in_bboxes([condition_value[0]]) and
                       flight.is_in_bboxes([condition_value[1]]))
            if not result:
                return False

        if 'changed_regions' in conditions:
            condition_value = conditions['changed_regions']
            # "any" (or True for backwards compat): trigger on any region change
            # "strict": only trigger if both prev and current are in some region
            mode = str(condition_value).lower() if condition_value is not True else "any"
            if mode == "strict":
                if not flight.was_in_any_bbox() or not flight.in_any_bbox():
                    return False
            result = flight.prev_inside_bboxes != flight.inside_bboxes
            if not result:
                return False

        if 'regions' in conditions:
            # KML region match
            condition_value = conditions['regions']
            result = flight.is_in_bboxes(condition_value)
            if not result:
                return False

        if 'cooldown' in conditions:
            # reduce firing rate to every n minutes
            cooldown_secs = int(conditions['cooldown'] * 60)  # convert to seconds
            result = not self.rule_execution_log.within_cooldown(rule_name,
                flight.flight_id, cooldown_secs, flight.lastloc.now)
            if not result:
                return False

        if 'rule_cooldown' in conditions:
            # reduce firing rate of this rule, regardless of aircraft
            cooldown_secs = int(conditions['rule_cooldown'] * 60)
            result = not self.rule_execution_log.within_cooldown(rule_name,
                                                                 "", cooldown_secs, 
                                                                 flight.lastloc.now)
            if not result:
                return False


        if 'latlongring' in conditions:
            condition_value = conditions['latlongring']
            dist = flight.lastloc.distfrom(
                condition_value[1], condition_value[2])
            result = condition_value[0] >= dist
            if not result:
                return False

        if 'has_attr' in conditions:
            condition_value = conditions['has_attr']
            if flight.lastloc.flightdict:
                result = condition_value in flight.lastloc.flightdict
            else:
                result = False
            if not result:
                return False

        if 'squawk' in conditions:
            condition_value = conditions['squawk']
            # condition_value can be a single squawk code or a list of codes
            if isinstance(condition_value, list):
                squawk_list = [str(s) for s in condition_value]
            else:
                squawk_list = [str(condition_value)]
            result = flight.lastloc.squawk in squawk_list
            if not result:
                return False

        if 'emergency' in conditions:
            condition_value = conditions['emergency']
            if flight.lastloc.emergency is None:
                return False
            # "any" matches any emergency status except "none"
            if condition_value == 'any':
                result = flight.lastloc.emergency != 'none'
            elif isinstance(condition_value, list):
                result = flight.lastloc.emergency in condition_value
            else:
                result = flight.lastloc.emergency == condition_value
            if not result:
                return False

        if 'category' in conditions:
            condition_value = conditions['category']
            if flight.lastloc.category is None:
                return False
            if isinstance(condition_value, list):
                result = flight.lastloc.category in condition_value
            else:
                result = flight.lastloc.category == condition_value
            if not result:
                return False

        if 'min_gs' in conditions:
            condition_value = conditions['min_gs']
            if flight.lastloc.gs is None:
                return False
            result = flight.lastloc.gs >= float(condition_value)
            if not result:
                return False

        if 'max_gs' in conditions:
            condition_value = conditions['max_gs']
            if flight.lastloc.gs is None:
                return False
            result = flight.lastloc.gs <= float(condition_value)
            if not result:
                return False

        if 'min_vertical_rate' in conditions:
            condition_value = conditions['min_vertical_rate']
            if flight.lastloc.baro_rate is None:
                return False
            result = flight.lastloc.baro_rate >= int(condition_value)
            if not result:
                return False

        if 'max_vertical_rate' in conditions:
            condition_value = conditions['max_vertical_rate']
            if flight.lastloc.baro_rate is None:
                return False
            result = flight.lastloc.baro_rate <= int(condition_value)
            if not result:
                return False

        if 'callsign_prefix' in conditions:
            condition_value = conditions['callsign_prefix']
            if flight.flight_id is None:
                return False
            if isinstance(condition_value, list):
                result = any(flight.flight_id.startswith(prefix) for prefix in condition_value)
            else:
                result = flight.flight_id.startswith(condition_value)
            if not result:
                return False

        if 'on_ground' in conditions:
            condition_value = conditions['on_ground']
            # on_ground: true means aircraft must be on ground
            # on_ground: false means aircraft must be airborne
            result = flight.lastloc.on_ground == condition_value
            if not result:
                return False

        if 'min_time' in conditions:
            condition_value = conditions['min_time']
            ts_24hr = int(datetime.datetime.utcfromtimestamp(
                flight.lastloc.now).strftime("%H%M"))
            result = ts_24hr >= condition_value
            if not result:
                return False

        if 'max_time' in conditions:
            condition_value = conditions['max_time']
            ts_24hr = int(datetime.datetime.utcfromtimestamp(
                flight.lastloc.now).strftime("%H%M"))
            result = ts_24hr <= condition_value
            if not result:
                return False

        if 'time_ranges' in conditions:
            ts_24hr = int(datetime.datetime.utcfromtimestamp(
                flight.lastloc.now).strftime("%H%M"))
            if not self._time_in_ranges(ts_24hr, conditions['time_ranges']):
                return False

        return True

    def _time_in_ranges(self, ts_24hr: int, time_ranges) -> bool:
        """
        Check if the integer time (HHMM, e.g. 1330) falls within any of the 
        specified time ranges.  Each range is a string like "0000-0130" 
        or "2200-0400" (wraps around midnight).
        """
        for rng in time_ranges:
            start_str, end_str = rng.split('-')
            start = int(start_str)
            end = int(end_str)
            if start <= end:
                # Normal range (e.g., 0900-1700)
                if start <= ts_24hr < end:
                    return True
            else:
                # Wraps around midnight (e.g., 2200-0400)
                if ts_24hr >= start or ts_24hr < end:
                    return True
        return False

    def actions_valid(self, actions: dict):
        """Check for invalid or unknown actions, return True if valid."""
        VALID_ACTIONS = ['webhook', 'print', 'callback', 'note', 'track',
                         'expire_callback', 'shell']

        for action in actions.keys():
            if action not in VALID_ACTIONS:
                logger.error("Unknown action: %s", action)
                return False
        return True

    def do_actions(self, flight: Flight, action_items: dict, rule_name: str,
                   cb_arg = None) -> None:
        """Rule matched, now execute the actions for the given flight."""

        self.rule_execution_log.log(rule_name, flight.flight_id,
                                    flight.lastloc.now,
                                    flight.flags.get('note', ''))

        for action_name, action_value in action_items.items():
            if 'webhook' == action_name:
                Stats.webhooks_fired += 1
                try:
                    [action_type, action_recipient] = action_value
                except Exception:  # pylint: disable=broad-except
                    logger.error("Invalid webhook action: %s", action_value)
                    continue

                # Build message based on webhook type
                if action_type == 'slack':
                    text = (f"Rule {rule_name} matched for: {flight.to_str()}\n"
                            f"LIVE LINK: {flight.to_link()}\n"
                            f"RECORDING: {flight.to_recording()}")
                else:
                    # Generic/page format
                    text = (f"Rule {rule_name}: {flight.lastloc.to_short_str()} "
                            f"{str(flight.inside_bboxes)}")

                if not send_webhook(action_type, action_recipient, text):
                    logger.debug("Webhook '%s' was not sent (not configured or failed)",
                                action_type)

            elif 'shell' == action_name:
                # Execute shell command with sanitized variable substitution
                try:
                    cmd = action_value.format(
                        flight_id=shlex.quote(flight.flight_id or ''),
                        hex=shlex.quote(flight.lastloc.hex or ''),
                        alt=int(flight.lastloc.alt_baro or 0),
                        lat=float(flight.lastloc.lat or 0),
                        lon=float(flight.lastloc.lon or 0),
                        speed=int(flight.lastloc.gs or 0),
                        track=int(flight.lastloc.track or 0),
                        rule=shlex.quote(rule_name),
                    )
                    logger.debug("Executing shell command: %s", cmd)
                    subprocess.run(cmd, shell=True, timeout=10,
                                   capture_output=True, check=False)
                except subprocess.TimeoutExpired:
                    logger.warning("Shell command timed out: %s", cmd[:50])
                except KeyError as e:
                    logger.error("Shell command has unknown variable: %s", e)
                except Exception as e:  # pylint: disable=broad-except
                    logger.error("Shell command failed: %s", e)

            elif 'print' == action_name:
                ts_utc = datetime.datetime.utcfromtimestamp(
                    flight.lastloc.now).strftime('%m/%d/%y %H:%M')
                note = flight.flags.get('note', '')
                msg = f"{rule_name}: {ts_utc} {flight.to_str()} {note}"
                if sys.stdout.isatty():
                    print(f"\033[92m{msg}\033[0m")  # green on TTY
                else:
                    print(msg)

            elif 'callback' == action_name:
                Stats.callbacks_fired += 1
                Stats.last_callback_flight = flight
                if not action_value in self.callbacks:
                    timestamp = datetime.datetime.fromtimestamp(
                        flight.lastloc.now).strftime("%m/%d/%y %H:%M")
                    other_flight_id = ""
                    if cb_arg:
                        other_flight_id = cb_arg.flight_id

                    logger.error("No callback defined: %s, %s, %s, %s",
                                 action_value, flight.flight_id, other_flight_id,
                                 timestamp)
                    continue

                logger.debug("Doing callback for %s", flight.flight_id)
                try:
                    if cb_arg:
                        # this is used for proximity events where you need to
                        # be able to refer to both flights that are near each other
                        self.callbacks[action_value](flight, cb_arg)
                    else:
                        # all non-proximity events go here
                        self.callbacks[action_value](flight)
                except TypeError as e:
                    logger.error("Callback %s arguments incorrect: %s",
                                 action_value, str(e))
            elif 'note' == action_name:
                # Attach a note to this flight for later use, typically in
                # another rule's callback.
                logger.debug("Setting note for %s to %s", flight.flight_id, action_value)
                flight.flags['note'] = action_value

            elif 'expire_callback' == action_name:
                # triggered on flight eviction from the system.
                pass # this is handled upon asynchronous expiration in do_expire()

            elif 'track' == action_name:
                # statistics gathering.
                pass # handled after execution is complete

            else:
                logger.warning("Unmatched action: %s", action_name)

    def do_expire(self, flight: Flight) -> None:
        """Handle flight expiration rules.
        
        The given flight is about to be evicted from the system, 
        see if any actions are needed.  This type of rule will be 
        needed for UI implementations at least.
        TODO: tests needed."""

        for rule_name, rule_value in self.get_rules().items():
            actions = rule_value['actions']

            if ( "expire_callback" in actions and
                self.conditions_match(flight, rule_value['conditions'], rule_name)):
                logger.debug("doing expire callback for %s", flight.flight_id)

                self.callbacks[actions['expire_callback']](flight)

    def get_rules_with_condition(self, condition_type) -> list:
        """Returns a list of name/rule tuples that have a condition of the given type."""

        rules_list = self.get_rules()
        ret = []
        for rule_name, rule_body in rules_list.items():
            if condition_type in rule_body['conditions']:
                ret.append((rule_name, rule_body))
        return ret

    def get_rules_with_action(self, action_type) -> list:
        """Returns a list of name/rule tuples that have an action of the given type."""

        rules_list = self.get_rules()
        ret = []
        for rule_name, rule_body in rules_list.items():
            if action_type in rule_body['actions']:
                ret.append((rule_name, rule_body))
        return ret

    def handle_proximity_conditions(self, flights, last_read_time) -> list:
        """
        This is run periodically to check distance between all aircraft --
        to check for any matching proximity conditions.  
        It's O(n^2), can be expensive, but altitude and bbox limits can help...

        NOTE: currently flights not in any bbox are not checked, to improve
        execution time.
        """

        prox_rules_list = self.get_rules_with_condition("proximity")
        found_prox_events = []

        if prox_rules_list == []:
            return

        for flight1 in flights.flight_dict.values():
            if flights.ignore_unboxed_flights and not flight1.in_any_bbox():
                continue

            for rule_name, rule_body in prox_rules_list:
                # For each proximity rule, we want to check the rule conditions
                # here, first removing the prox part of the rule which will
                # never match during the usual synchronous update.
                rule_conditions = rule_body['conditions'].copy()
                altsep, latsep = rule_conditions['proximity']
                del rule_conditions['proximity']

                if self.conditions_match(flight1, rule_conditions, rule_name):
                    # Satisfied prox rule found, now see if there are nearby aircraft.
                    # NOTE that this only returns one flight, so we won't always have
                    # two actions fired for every pair of close-proximity aircraft.
                    flight2 = flights.find_nearby_flight(flight1, altsep, latsep,
                                                         last_read_time)
                    if flight2:
                        # Also check if flight2 matches the rule conditions
                        # This ensures excluded aircraft in flight2 won't
                        # trigger the rule.
                        if self.conditions_match(flight2, rule_conditions, rule_name):
                            logger.debug("Proximity match: %s %s", flight1.flight_id,
                                        flight2.flight_id)
                            self.do_actions(flight1, rule_body['actions'], rule_name,
                                            flight2)
                            found_prox_events.append((flight1, flight2))
        return found_prox_events

    def print_final_report(self):
        """Print a report of rule execution statistics, for any rule
        that contains a "track" action."""

        tracked_rules = self.get_rules_with_action("track")
        for rule_name, _ in tracked_rules:
            log = self.rule_execution_log
            if rule_name in log.rule_execution_counters:
                counter = log.rule_execution_counters[rule_name]
            else:
                counter = ExecutionCounter(rule_name)

            counter.print_report()
