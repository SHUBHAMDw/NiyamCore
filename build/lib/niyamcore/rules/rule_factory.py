# niyamcore/rules/rule_factory.py

import logging
from typing import Type
from niyamcore.rules.base_rule import BaseValidationRule
from niyamcore.rules.not_null_rule import NotNullRule
from niyamcore.rules.range_rule import RangeRule
from niyamcore.rules.unique_rule import UniqueRule # Import your unique rule

logger = logging.getLogger(__name__)

# Dictionary mapping rule type strings to their corresponding Rule classes
RULE_CLASSES = {
    "not_null": NotNullRule,
    "range": RangeRule,
    "unique": UniqueRule,
    # Add other rule types here as you implement them
    # "is_numeric": IsNumericRule,
    # "before_or_equal_to_current_date": BeforeOrEqualToCurrentDateRule,
}

def create_rule(rule_config: dict) -> BaseValidationRule:
    """
    Factory function to create a concrete validation rule instance
    based on the 'type' specified in the rule configuration.

    Args:
        rule_config (dict): A dictionary containing the rule's configuration,
                            including 'type', 'severity', 'error_message',
                            and rule-specific parameters.

    Returns:
        BaseValidationRule: An instance of the appropriate concrete rule class.

    Raises:
        ValueError: If an unknown rule type is provided or required parameters are missing.
    """
    rule_type = rule_config.get("type")
    if not rule_type:
        raise ValueError("Rule configuration is missing 'type' field.")

    rule_class = RULE_CLASSES.get(rule_type.lower())
    if not rule_class:
        raise ValueError(f"Unknown validation rule type: '{rule_type}'. Available types: {list(RULE_CLASSES.keys())}")

    severity = rule_config.get("severity")
    error_message = rule_config.get("error_message")
    when_condition = rule_config.get("when_condition")

    if not severity:
        logger.warning(f"Rule type '{rule_type}' is missing 'severity'. Defaulting to 'ERROR'.")
        severity = "ERROR" # Default severity
    if not error_message:
        logger.warning(f"Rule type '{rule_type}' is missing 'error_message'. Using default.")
        error_message = f"Validation failed for {rule_type} rule."

    # Pass all remaining rule_config items as kwargs to the rule's constructor
    # Filter out common base rule parameters to avoid passing them twice
    filtered_kwargs = {k: v for k, v in rule_config.items() if k not in ["type", "severity", "error_message", "when_condition"]}

    try:
        rule_instance = rule_class(
            severity=severity,
            error_message=error_message,
            when_condition=when_condition,
            **filtered_kwargs
        )
        logger.debug(f"Created rule instance: {rule_type} with severity {severity}")
        return rule_instance
    except TypeError as e:
        raise ValueError(f"Missing or invalid parameters for rule type '{rule_type}': {e}. Config: {rule_config}")
    except Exception as e:
        raise ValueError(f"Error creating rule instance for type '{rule_type}': {e}. Config: {rule_config}")