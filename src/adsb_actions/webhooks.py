"""Pluggable webhook system for notifications.

This module provides a registry pattern for webhook handlers, allowing
users to add custom notification types beyond the built-in Slack and
paging support.

Example usage in a callback file:
    from adsb_actions.webhooks import register_webhook_handler

    def my_discord_webhook(recipient: str, message: str) -> bool:
        # Your Discord integration code here
        return True

    register_webhook_handler('discord', my_discord_webhook)

Then in YAML:
    rules:
      my_rule:
        actions:
          webhook: ['discord', 'my_channel']
"""

import logging

logger = logging.getLogger(__name__)

# Registry of webhook handlers: name -> handler function
WEBHOOK_HANDLERS: dict = {}


def register_webhook_handler(name: str, handler_fn):
    """Register a custom webhook handler.

    Args:
        name: Webhook type name (used in YAML config, e.g., 'slack', 'page', 'discord')
        handler_fn: Function(recipient: str, message: str) -> bool
                   Should return True on success, False on failure.

    Example:
        def my_handler(recipient, message):
            # send notification
            return True
        register_webhook_handler('myservice', my_handler)
    """
    logger.debug("Registering webhook handler: %s", name)
    WEBHOOK_HANDLERS[name] = handler_fn


def send_webhook(webhook_type: str, recipient: str, message: str) -> bool:
    """Dispatch a webhook to the registered handler.

    Args:
        webhook_type: The type of webhook (e.g., 'slack', 'page')
        recipient: The recipient identifier (channel name, pager ID, etc.)
        message: The message to send

    Returns:
        True if the webhook was sent successfully, False otherwise.
        Returns False if no handler is registered for the webhook type.
    """
    if webhook_type not in WEBHOOK_HANDLERS:
        logger.warning("Webhook type '%s' not registered (skipping). "
                      "Available types: %s", webhook_type, list(WEBHOOK_HANDLERS.keys()))
        return False

    try:
        return WEBHOOK_HANDLERS[webhook_type](recipient, message)
    except Exception as e:  # pylint: disable=broad-except
        logger.error("Webhook '%s' failed: %s", webhook_type, e)
        return False


def get_registered_handlers() -> list:
    """Return list of registered webhook handler names."""
    return list(WEBHOOK_HANDLERS.keys())
