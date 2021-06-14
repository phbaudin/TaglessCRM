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

"""Tests for tcrm.hooks.ga4_hook."""

import json
import unittest
import unittest.mock as mock

import requests

from plugins.pipeline_plugins.hooks import ga4_hook
from plugins.pipeline_plugins.utils import blob
from plugins.pipeline_plugins.utils import errors


class GoogleAnalyticsV4HookTest(unittest.TestCase):

  def setUp(self):
    """Setup function for each unit test."""

    super(GoogleAnalyticsV4HookTest, self).setUp()

    self.test_api_secret = 'abcdABCDefghEFGH123456'
    self.test_payload_type_gtag = 'gtag'
    self.test_payload_type_firebase = 'firebase'
    self.test_measurement_id = 'G-02ABCDEFGH'
    self.test_firebase_app_id = '1:12345678901:android:1234567890abcdef'

    self.test_gtag_hook = ga4_hook.GoogleAnalyticsV4Hook(
        self.test_api_secret,
        self.test_payload_type_gtag,
        measurement_id=self.test_measurement_id)

    self.test_firebase_hook = ga4_hook.GoogleAnalyticsV4Hook(
        self.test_api_secret,
        self.test_payload_type_firebase,
        firebase_app_id=self.test_firebase_app_id)

    self.gtag_full_url = (f'https://www.google-analytics.com/mp/collect?'
                          f'api_secret={self.test_api_secret}&'
                          f'measurement_id={self.test_measurement_id}')
    self.firebase_full_url = (f'https://www.google-analytics.com/mp/collect?'
                              f'api_secret={self.test_api_secret}&'
                              f'firebase_app_id={self.test_firebase_app_id}')
    self.test_client_id = 'test_client_id'
    self.test_app_instance_id = 'test_app_instance_id'
    self.test_ga4_payload = {
        'client_id':
            'cid',
        'events': [{
            'name': 'add_to_cart',
            'params': {
                'quantity': 1,
                'item_name': 'good shoes',
                'item_id': 'shoe_id_1',
                'price': 5999,
                'currency': 'JPY'
            }
        }]
    }
    self.test_event = {
        'id': 1,
        'payload': json.dumps(self.test_ga4_payload)
    }

  def test_ga4_hook_init_with_no_api_secret(self):
    with self.assertRaises(errors.DataOutConnectorError):
      self.test_hook = ga4_hook.GoogleAnalyticsV4Hook(
          None, 'gtag', measurement_id=self.test_measurement_id)

  def test_ga4_hook_init_with_gtag_payload_type_and_firebase_app_id(self):
    with self.assertRaises(errors.DataOutConnectorError):
      self.test_hook = ga4_hook.GoogleAnalyticsV4Hook(
          self.test_api_secret,
          'gtag',
          firebase_app_id=self.test_firebase_app_id)

  def test_ga4_hook_init_with_firebase_payload_type_and_measurement_id(self):
    with self.assertRaises(errors.DataOutConnectorError):
      self.test_hook = ga4_hook.GoogleAnalyticsV4Hook(
          self.test_api_secret,
          'firebase',
          measurement_id=self.test_measurement_id)

  def test_ga4_hook_init_with_unsupport_payload_type(self):
    with self.assertRaises(errors.DataOutConnectorError):
      self.test_hook = ga4_hook.GoogleAnalyticsV4Hook(self.test_api_secret,
                                                      'unsupported_type')

  def test_ga4_hook_send_event_with_illegal_json(self):
    test_event = {'id': 1, 'payload': '{'}
    blb = blob.Blob(events=[test_event], location='')

    blb = self.test_gtag_hook.send_events(blb)
    self.assertEqual(len(blb.failed_events), 1)
    self.assertEqual(
        blb.failed_events[0][2],
        errors.ErrorNameIDMap.GA4_HOOK_ERROR_INVALID_JSON_STRUCTURE.value)

  def payload_validation_case(self,
                              ga4_error_value,
                              field_path=None,
                              description=''):
    validation_messages = {
        'validationMessages': [{
            'fieldPath': field_path,
            'description': description
        }]
    }
    if not field_path:
      validation_messages['validationMessages'][0]['fieldPath'] = field_path

    blb = blob.Blob(events=[self.test_event], location='')

    with mock.patch('requests.post') as mock_resp:
      mock_resp.return_value = mock.MagicMock()
      mock_resp.return_value.status_code = 200
      mock_resp.return_value.json.return_value = validation_messages

      blb = self.test_gtag_hook.send_events(blb)
      self.assertEqual(len(blb.events), 1)
      self.assertEqual(len(blb.failed_events), 1)
      for failed_event in blb.failed_events:
        self.assertEqual(failed_event[2], ga4_error_value)

  def test_ga4_hook_send_event_with_no_client_id(self):
    self.payload_validation_case(
        errors.ErrorNameIDMap.GA4_HOOK_ERROR_VALUE_REQUIRED_CLIENT_ID.value,
        'client_id')

  def test_ga4_hook_send_event_with_invalid_timestamp(self):
    self.payload_validation_case(
        errors.ErrorNameIDMap.GA4_HOOK_ERROR_VALUE_INVALID_TIMESTAMP_MICROS
        .value, 'timestamp_micros')
    self.payload_validation_case(
        errors.ErrorNameIDMap.GA4_HOOK_ERROR_VALUE_INVALID_TIMESTAMP_MICROS
        .value,
        description='(timestamp_micros)')

  def test_ga4_hook_send_event_with_invalid_non_personalized_ads(self):
    self.payload_validation_case(
        errors.ErrorNameIDMap.GA4_HOOK_ERROR_VALUE_INVALID_NON_PERSONALIZED_ADS
        .value,
        description='(non_personalized_ads)')

  def test_ga4_hook_send_event_with_invalid_user_id(self):
    self.payload_validation_case(
        errors.ErrorNameIDMap.GA4_HOOK_ERROR_VALUE_INVALID_USER_ID.value,
        description='(user_id)')

  def test_ga4_hook_send_event_with_invalid_user_properties(self):
    self.payload_validation_case(
        errors.ErrorNameIDMap.GA4_HOOK_ERROR_VALUE_INVALID_USER_PROPERTIES
        .value,
        description='(user_properties[0].value)')

  def test_ga4_hook_send_event_with_invalid_events(self):
    self.payload_validation_case(
        errors.ErrorNameIDMap.GA4_HOOK_ERROR_VALUE_INVALID_EVENTS.value,
        description='(events[0])')

  def test_ga4_hook_send_event_with_invalid_events_params(self):
    self.payload_validation_case(
        errors.ErrorNameIDMap.GA4_HOOK_ERROR_VALUE_INVALID_EVENTS_PARAMS.value,
        field_path='events.params')

  def test_ga4_hook_send_event_with_invalid_events_params_items(self):
    self.payload_validation_case(
        (errors.ErrorNameIDMap.GA4_HOOK_ERROR_VALUE_INVALID_EVENTS_PARAMS_ITEMS
         .value),
        field_path='events.params.items')

  def test_ga4_hook_send_event_with_invalid_ga4_payload(self):
    self.payload_validation_case(
        errors.ErrorNameIDMap.GA4_HOOK_ERROR_INVALID_VALUES.value)

  def test_ga4_hook_send_event_with_retriable_http_error(self):
    blb = blob.Blob(events=[self.test_event], location='')
    with mock.patch('requests.post') as mock_resp:
      mock_resp.side_effect = requests.ConnectionError
      blb = self.test_gtag_hook.send_events(blb)
      self.assertEqual(len(blb.failed_events), 1)
      self.assertEqual(
          blb.failed_events[0][2],
          errors.ErrorNameIDMap.RETRIABLE_GA4_HOOK_ERROR_HTTP_ERROR.value)

  def test_ga4_hook_send_event(self):
    blb = blob.Blob(events=[self.test_event], location='')

    with mock.patch('requests.post') as mock_resp:
      mock_resp.return_value = mock.MagicMock()
      mock_resp.return_value.status_code = 200
      mock_resp.return_value.json.return_value = {'validationMessages': []}

      blb = self.test_gtag_hook.send_events(blb)
      self.assertEqual(len(blb.events), 1)
      self.assertEqual(len(blb.failed_events), 0)


if __name__ == '__main__':
  unittest.main()
