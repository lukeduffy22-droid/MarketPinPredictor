"""Versioned primary gamma gates; constants are shared by execution and labels.

This identifies calculation validation, never forecast accuracy or live eligibility.
Changing these gates requires a new method version and reviewed evidence.
"""
import hashlib
import json

METHOD_ID = 'databento-primary-gamma-gates-v1'
MONEYNESS_LOWER = 0.96
MONEYNESS_UPPER = 1.04
MIN_PRIMARY_STRIKES = 30
MIN_NONZERO_STRIKES = 10
MAX_STRIKE_CONCENTRATION = 0.9


def validation_method_fields(min_paired_quotes):
    policy = {'method_id':METHOD_ID, 'moneyness_lower':MONEYNESS_LOWER,
              'moneyness_upper':MONEYNESS_UPPER,'min_primary_strikes':MIN_PRIMARY_STRIKES,
              'min_nonzero_strikes':MIN_NONZERO_STRIKES,
              'max_strike_concentration':MAX_STRIKE_CONCENTRATION,
              'min_paired_quotes':min_paired_quotes,
              'scope':'primary_gamma_calculation_gates_not_prediction_or_accuracy'}
    digest = hashlib.sha256(json.dumps(policy,sort_keys=True,separators=(',',':')).encode()).hexdigest()
    return {'validation_method_id':METHOD_ID, 'validation_policy':policy,
            'validation_policy_sha256':digest}


def validation_labels(record):
    """Derived labels only: never assign today's method to an old record."""
    method = record.get('validation_method_id')
    policy = record.get('validation_policy')
    classification = 'METHOD_UNRECORDED'
    if method:
        classification = 'METHOD_UNRECOGNIZED'
        if method == METHOD_ID and isinstance(policy,dict):
            paired = policy.get('min_paired_quotes')
            if type(paired) is int and paired > 0:
                expected = validation_method_fields(paired)
                if policy == expected['validation_policy'] and record.get('validation_policy_sha256') == expected['validation_policy_sha256']:
                    classification = 'METHOD_RECORDED'
                else:
                    classification = 'METHOD_METADATA_INVALID'
    valid = record.get('validation_is_valid') is True and record.get('gamma_excluded_from_model') is False
    return {'review_validation_method':classification,
            'review_validation_method_id':str(method or 'unrecorded'),
            'review_validation_policy_sha256':str(record.get('validation_policy_sha256') or 'unrecorded'),
            'review_calculation_status':'PRODUCER_PASS' if valid else 'FAILED_OR_UNPROVEN',
            'review_accuracy_status':'NOT_ESTABLISHED',
            'review_revalidated':False}
