#!/usr/bin/env/python

import json
import os
import singer
from singer import metadata
import requests
from requests.auth import HTTPBasicAuth

from tap_klaviyo.utils import get_incremental_pull, get_full_pulls, get_all_pages

ENDPOINTS = {
    'global_exclusions': 'https://a.klaviyo.com/api/v1/people/exclusions',
    'lists': 'https://a.klaviyo.com/api/v1/lists',
    # to get list of available metrics
    'metrics': 'https://a.klaviyo.com/api/v1/metrics',
    # to get individual metric data
    'metric': 'https://a.klaviyo.com/api/v1/metric/',
    # to get list members
    'list_members': 'https://a.klaviyo.com/api/v2/group/{list_id}/members/all',
    'events': 'https://a.klaviyo.com/api/events/',
    'profiles': 'https://a.klaviyo.com/api/profiles/',
}

# listing of incremental streams
EVENT_MAPPINGS = {
    "Received Email": "receive",
    "Clicked Email": "click",
    "Opened Email": "open",
    "Bounced Email": "bounce",
    "Unsubscribed": "unsubscribe",
    "Marked Email as Spam": "mark_as_spam",
    "Unsubscribed from List": "unsub_list",
    "Subscribed to List": "subscribe_list",
    "Updated Email Preferences": "update_email_preferences",
    "Dropped Email": "dropped_email",
    "Events": "events",
    "Profiles":"profiles"
}


class Stream(object):
    def __init__(self, stream, tap_stream_id, key_properties, puller):
        self.stream = stream
        self.tap_stream_id = tap_stream_id
        self.key_properties = key_properties
        self.puller = puller

    def to_catalog_dict(self):
        return {
            'stream': self.stream,
            'tap_stream_id': self.tap_stream_id,
            'key_properties': self.key_properties,
            'schema': load_schema(self.stream),
            'metadata': build_metadata(self.stream, self.key_properties)
        }


# CREDENTIALS_KEYS = ["api_key"]
REQUIRED_CONFIG_KEYS = ["start_date"] #+ CREDENTIALS_KEYS

GLOBAL_EXCLUSIONS = Stream(
    'global_exclusions',
    'global_exclusions',
    'email',
    'full'
)

LISTS = Stream(
    'lists',
    'lists',
    'uuid',
    'lists'
)

LIST_MEMBERS = Stream(
    'list_members',
    'list_members',
    'email',
    'full'
)

EVENTS = Stream(
    'events',
    'events',
    'uuid',
    'full'
)

PROFILES = Stream(
    'profiles',
    'profiles',
    'id',
    'full'
)

FULL_STREAMS = [GLOBAL_EXCLUSIONS, LISTS, LIST_MEMBERS, EVENTS,PROFILES]


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schema(name):
    return json.load(open(get_abs_path('schemas/{}.json'.format(name))))


def build_metadata(name, key_properties):
    schema = load_schema(name)

    mdata = metadata.new()
    mdata = metadata.write(mdata, (), 'table-key-properties', key_properties)

    for field in schema["properties"].keys():
        mdata = metadata.write(mdata, ('properties', field), 'inclusion', 'available')

    return metadata.to_list(mdata)

def stream_is_selected(mdata):
    return mdata.get((), {}).get('selected', False)

def do_sync(config, state, catalog):
    api_key = config["credentials"]["api_key"]
    list_ids = config.get('list_ids')
    start_date = config['start_date'] if 'start_date' in config else None

    selected_streams = []
    for stream in catalog['streams']:
        mdata = metadata.to_map(stream.get('metadata'))
        if stream_is_selected(mdata):
            selected_streams.append(stream)

    for stream in selected_streams:
        singer.write_schema(
            stream['stream'],
            stream['schema'],
            stream['key_properties']
        )
        if stream['stream'] in EVENT_MAPPINGS.values():
            get_incremental_pull(stream, ENDPOINTS, state,
                                 api_key, start_date)
        elif stream['stream'] == 'list_members':
            if not list_ids:
                list_ids = []
                for response in get_all_pages("lists", ENDPOINTS["lists"], api_key):
                    records = response.json().get('data')
                    list_ids.extend([r["id"] for r in records])
            get_full_pulls(stream, ENDPOINTS[stream['stream']], api_key, list_ids)
        else:
            get_full_pulls(stream, ENDPOINTS[stream['stream']], api_key)


def get_available_metrics(api_key):
    metric_streams = []
    for response in get_all_pages('metric_list',
                                  ENDPOINTS['metrics'], api_key):
        for metric in response.json().get('data'):
            if metric['name'] in EVENT_MAPPINGS:
                metric_streams.append(
                    Stream(
                        stream=EVENT_MAPPINGS[metric['name']],
                        tap_stream_id=metric['id'],
                        key_properties="id",
                        puller='incremental'
                    )
                )

    return metric_streams


def discover(api_key):
    metric_streams = get_available_metrics(api_key)
    return {"streams": [a.to_catalog_dict()
                        for a in metric_streams + FULL_STREAMS]}


def do_discover(api_key):
    print(json.dumps(discover(api_key), indent=2))


def refresh_credentials(config):
    body = {"grant_type": "refresh_token",
            "refresh_token": config["refresh_token"]}

    resp = requests.post("https://a.klaviyo.com/oauth/token", data=body, auth=HTTPBasicAuth(config["client_id"], config["client_secret"]))
    resp.raise_for_status()
    config["access_token"] = resp.json()['access_token']

    # TODO: Need to refresh the token in case of long running jobs
    # login_timer = threading.Timer(REFRESH_TOKEN_EXPIRATION_PERIOD, refresh_credentials)
    # login_timer.start()


def main():
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    # Refresh the access_token if refresh_token is present
    # if args.config.get("refresh_token"):
    #     refresh_credentials(args.config)

    access_token = args.config["credentials"]["api_key"]

    if args.discover:
        do_discover(access_token)

    else:
        catalog = args.catalog if args.catalog else discover(access_token)
        state = args.state if args.state else {"bookmarks": {}}
        do_sync(args.config, state, catalog.to_dict())


if __name__ == '__main__':
    main()
