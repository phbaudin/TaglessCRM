# python3
# coding=utf-8
# Copyright 2020 Google LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Custom Hook for sending store sales conversions to Google Ads via Adwords API."""
import re
from typing import Any, Callable, Dict, Generator, List, Optional, Tuple

from plugins.pipeline_plugins.hooks import ads_hook
from plugins.pipeline_plugins.hooks import output_hook_interface
from plugins.pipeline_plugins.utils import blob
from plugins.pipeline_plugins.utils import errors

_DEFAULT_BATCH_SIZE = 1000
_SHA256_DIGEST_PATTERN = r'^[A-Fa-f0-9]{64}$'

# Conversion time should follow below pattern for avoiding
# INVALID_STRING_DATE_TIME error, for detail:
# https://developers.google.com/adwords/api/docs/reference/v201809/OfflineDataUploadService.DateError
# Good date time string example: 20191030 122301 Asia/Calcutta
_RE_STRING_DATE_TIME = r'\d{8} \d{6} [\w\/-]+'


def _validate_sha256_pattern(field_data: str) -> None:
  """Validates if field_data matches sha256 digest string pattern.

  The correct pattern is '^[A-Fa-f0-9]{64}$'
  Note: None is an invalid sha256 value

  Args:
    field_data: A field data which is a part of member data entity of Google
                Adwords API

  Raises:
    DataOutConnectorValueError: If the any field data is invalid or None.
  """
  if field_data is None or not re.match(_SHA256_DIGEST_PATTERN, field_data):
    raise errors.DataOutConnectorValueError(
        'None or string is not in SHA256 format.', errors.ErrorNameIDMap.
        ADS_SSD_HOOK_ERROR_PAYLOAD_FIELD_VIOLATES_SHA256_FORMAT)


class GoogleAdsStoreSalesConversionsHook(
    ads_hook.GoogleAdsHook, output_hook_interface.OutputHookInterface):
  """Custom hook to send store sales conversions to Google Ads via Adwords API."""

  def __init__(self, ads_ssd_external_upload_id: str, ads_credentials: str,
               **kwargs) -> None:
    """Initialize with a specified external_upload_id.

    Args:
      ads_ssd_external_upload_id: The external upload ID.
      ads_credentials: A dict of Adwords client ids and tokens.
        Reference for desired format:
          https://developers.google.com/adwords/api/docs/guides/first-api-call
      **kwargs: Other optional arguments.

    Raises:
      DataOutConnectorValueError if any of the following happens.
        - ads_ssd_external_upload_id is empty.
    """
    super().__init__(ads_yaml_doc=ads_credentials)

    self._validate_init_params(ads_ssd_external_upload_id)
    self.external_upload_id = ads_ssd_external_upload_id

  def _validate_init_params(self, external_upload_id: str) -> None:
    """Validate the external_upload_id parameter.

    Args:
      external_upload_id: The external upload ID.

    Raises:
      DataOutConnectorValueError if external_upload_id is empty.
    """
    if not external_upload_id:
      raise errors.DataOutConnectorValueError(
          'External upload ID is empty.',
          errors.ErrorNameIDMap.ADS_SSD_HOOK_ERROR_EMPTY_UPLOAD_ID)

  def _format_event(self, event: Dict[Any, Any]) -> Dict[Any, Any]:
    """Format a contact_info event.

    Args:
      event: A raw contact_info event.

    Returns:
      A formatted contact_info event.

    Raises:
      DataOutConnectorValueError for the following scenarios:
        - If filed hashedEmail and hashedPhoneNumber not
          exist in the payload.
        - hashedEmail or hashedPhoneNumber fields do not meet SHA256 format.
    """
    transaction = {}

    store_sales_transaction_fields = [
        'transactionTime', 'microAmount', 'currencyCode', 'conversionName'
    ]

    if not all(
        field in event.keys() for field in store_sales_transaction_fields):
      raise errors.DataOutConnectorValueError(
          f'Event is missing at least one mandatory field'
          f' {[field for field in store_sales_transaction_fields]}',
          errors.ErrorNameIDMap.ADS_SSD_HOOK_ERROR_MISSING_MANDATORY_FIELDS)

    identifier_types = {
        'email': 'HASHED_EMAIL',
        'email2': 'HASHED_EMAIL',
        'email3': 'HASHED_EMAIL',
        'firstName': 'HASHED_LAST_NAME',
        'lastName': 'HASHED_FIRST_NAME',
        'city': 'CITY',
        'state': 'STATE',
        'zip': 'ZIPCODE',
        'country': 'COUNTRY_CODE',
        'phoneNumber': 'HASHED_PHONE',
        'phoneNumber2': 'HASHED_PHONE',
        'phoneNumber3': 'HASHED_PHONE',
    }

    hashed_types = [
        'HASHED_EMAIL', 'HASHED_PHONE', 'HASHED_LAST_NAME', 'HASHED_FIRST_NAME'
    ]

    user_identifiers = []

    for field, type in identifier_types.items():
      if field in event.keys():
        if type in hashed_types:
          _validate_sha256_pattern(event[field])

        user_identifiers.append({
            'userIdentifierType': type,
            'value': event[field],
        })

    if not user_identifiers:
      raise errors.DataOutConnectorValueError(
          f'Event is missing at least one user identifier field'
          f' {[field for field in identifier_types.keys()]}',
          errors.ErrorNameIDMap.ADS_SSD_HOOK_ERROR_MISSING_IDENTIFIER_FIELD)

    transaction['userIdentifiers'] = user_identifiers

    if not re.match(_RE_STRING_DATE_TIME, event['transactionTime']):
      raise errors.DataOutConnectorValueError(
          'transactionTime should be formatted: yyyymmdd hhmmss [tz]', errors.
          ErrorNameIDMap.ADS_SSD_HOOK_ERROR_INVALID_FORMAT_OF_CONVERSION_TIME)

    transaction['transactionTime'] = event['transactionTime']

    if not event['microAmount'].isdigit():
      raise errors.DataOutConnectorValueError(
          'microAmount must be a positive integer.',
          errors.ErrorNameIDMap.ADS_SSD_HOOK_ERROR_INVALID_CONVERSION_VALUE)

    if not event['currencyCode']:
      raise errors.DataOutConnectorValueError(
          'Currency code must not be empty.',
          errors.ErrorNameIDMap.ADS_SSD_HOOK_ERROR_INVALID_CURRENCY)

    transaction['transactionAmount'] = {
        'money': {
            'microAmount': event['microAmount'],
        },
        'currencyCode': event['currencyCode'],
    }

    if not event['conversionName'] or len(event['conversionName']) > 100:
      raise errors.DataOutConnectorValueError(
          'Length of conversionName should be <= 100.', errors.ErrorNameIDMap.
          ADS_SSD_HOOK_ERROR_INVALID_LENGTH_OF_CONVERSION_NAME)

    transaction['conversionName'] = event['conversionName']

    return {'StoreSalesTransaction': transaction}

  def _validate_and_prepare_events_to_send(
      self, events: List[Dict[str, Any]]
  ) -> Tuple[List[Tuple[int, Dict[str, Any]]], List[Tuple[
      int, errors.ErrorNameIDMap]]]:
    """Converts events to correct format before sending.

    Reference for the correct format:
    https://developers.google.com/adwords/api/docs/reference/v201809/OfflineDataUploadService.StoreSalesTransaction

    Args:
      events: All unformated events.

    Returns:
      members: Formated events.
    """
    valid_events = []
    invalid_indices_and_errors = []

    for i, event in enumerate(events):
      try:
        payload = self._format_event(event)
      except errors.DataOutConnectorValueError as error:
        invalid_indices_and_errors.append((i, error.error_num))
      else:
        valid_events.append((i, payload))

    return valid_events, invalid_indices_and_errors

  def _batch_generator(
      self, events: List[Tuple[int, Dict[str, Any]]]
  ) -> Generator[List[Tuple[int, Dict[str, Any]]], None, None]:
    """Splits conversion events into batches of _CONVERSION_BATCH_MAX_SIZE.

    AdWords API batch constraints can be found at:
    https://developers.google.com/adwords/api/docs/reference/v201809/AdwordsUserListService.MutateMembersOperand

    Args:
      events: Indexed events to send.

    Yields:
      List of batches of events. Each batch is of _CONVERSION_BATCH_MAX_SIZE.
    """
    for i in range(0, len(events), _DEFAULT_BATCH_SIZE):
      yield events[i:i + _DEFAULT_BATCH_SIZE]

  def send_events(self, blb: blob.Blob) -> blob.Blob:
    """Sends Customer Match events to Google AdWords API.

    Args:
      blb: A blob containing Customer Match data to send.

    Returns:
      A blob containing updated data about any failing events or reports.
    """
    valid_events, invalid_indices_and_errors = (
        self._validate_and_prepare_events_to_send(blb.events))
    batches = self._batch_generator(valid_events)

    for batch in batches:
      try:
        user_list = [event[1] for event in batch]
        self.add_store_sales_conversions(self.external_upload_id, user_list,
                                         self.upload_type)
      except errors.DataOutConnectorSendUnsuccessfulError as error:
        for event in batch:
          invalid_indices_and_errors.append((event[0], error.error_num))

    for event in invalid_indices_and_errors:
      blb.append_failed_event(event[0] + blb.position, blb.events[event[0]],
                              event[1].value)

    return blb
