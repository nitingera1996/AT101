import requests
import os
import json
import dateutil.parser
from utils.dump_utils import dump_signal
from helpers.sentiment_analysis import analyze_sentiment

TWITTER_SIGNAL_NAME = "TWITTER"

# TODO: Take user accounts from config and set as rules
OUR_RULES = [
    {"value": "from:nitingera1996", "tag": "devo"},
    {"value": "from:elonmusk", "tag": "elon"},
    {"value": "from:WuBlockchain", "tag": "major_news"},
    {"value": "from:APompliano OR from:scottmelker OR from:michael_saylor OR from:cameron OR from:brian_armstrong", "tag": "tier_2"},
    # {"value": "#bitcoin -is:retweet -is:quote", "tag": "generic"},
]

CRYPTO_WORDS = ["moon", "crypto", "bitcoin", "ethereum"]

# Tweet fields are adjustable.
# Options include:
# attachments, author_id, context_annotations,
# conversation_id, created_at, entities, geo, id,
# in_reply_to_user_id, lang, non_public_metrics, organic_metrics,
# possibly_sensitive, promoted_metrics, public_metrics, referenced_tweets,
# source, text, and withheld
# TODO: See if we can read attachments
TWEET_FIELDS = "tweet.fields=lang,created_at,public_metrics"
LIKE_COUNT_THRESHOLD = 500


def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers


def get_rules(headers):
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream/rules", headers=headers
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot get rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print(f"Got rules - {json.dumps(response.json())}")
    return response.json()


def delete_all_rules(headers, rules):
    if rules is None or "data" not in rules:
        return None

    ids = list(map(lambda rule: rule["id"], rules["data"]))
    payload = {"delete": {"ids": ids}}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        headers=headers,
        json=payload
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot delete rules (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    print(json.dumps(response.json()))


def set_rules(headers, rules):
    payload = {"add": rules}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        headers=headers,
        json=payload,
    )
    if response.status_code != 201:
        raise Exception(
            "Cannot add rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print(f"Rules added - {json.dumps(response.json())}")


def listen_to_stream(headers):
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream", headers=headers, stream=True,
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot get stream (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    for response_line in response.iter_lines():
        if response_line:
            try:
                json_response = json.loads(response_line)
                print(json_response)
                tweet_id = json_response["data"]["id"]
                tweet_lookup_url = create_lookup_url([tweet_id])
                full_tweet = connect_to_endpoint(tweet_lookup_url, headers)
                print(json.dumps(full_tweet, indent=4, sort_keys=True))
                check_and_dump_signal(full_tweet, json_response["matching_rules"][0]["tag"])
            except Exception as e:
                print(e)


def check_and_dump_signal(full_tweet, tag):
    # TODO : Find Coin in text.
    # Can add strategies as per tags here
    print(f'Tag: {tag}')
    if tag == "elon" or tag == "devo":
        # Dump everything without checking much for now
        full_tweet_data = full_tweet["data"][0]
        tweet_text = full_tweet_data["text"]
        tweet_lang = full_tweet_data["lang"]
        if tweet_lang == "en" and any(map(tweet_text.lower().__contains__, CRYPTO_WORDS)):
            polarity = analyze_sentiment(tweet_text)
            dump_signal(TWITTER_SIGNAL_NAME, dateutil.parser.isoparse(full_tweet_data["created_at"]),
                        full_tweet_data["id"], sentiment=polarity)
    elif tag == "major_news" or tag == "tier_2":
        # Dump everything without checking much for now
        full_tweet_data = full_tweet["data"][0]
        tweet_text = full_tweet_data["text"]
        tweet_lang = full_tweet_data["lang"]
        if tweet_lang == "en" and any(map(tweet_text.lower().__contains__, CRYPTO_WORDS)):
            dump_signal(TWITTER_SIGNAL_NAME, dateutil.parser.isoparse(full_tweet_data["created_at"]), full_tweet_data["id"])
    else:
        full_tweet_data = full_tweet["data"][0]
        like_count = full_tweet_data["public_metrics"]["like_count"]
        if like_count > LIKE_COUNT_THRESHOLD:
            dump_signal(TWITTER_SIGNAL_NAME, dateutil.parser.isoparse(full_tweet_data["created_at"]), full_tweet_data["id"])


def connect_to_endpoint(url, headers):
    response = requests.request("GET", url, headers=headers)
    if response.status_code != 200:
        raise Exception(
            "Request returned an error: {} {}".format(
                response.status_code, response.text
            )
        )
    return response.json()


def create_lookup_url(ids):
    """
    :param ids: list of ids
                You can adjust ids to include a single Tweets.
                Or you can add to up to 100 comma-separated IDs
    :return: url
    """
    ids_url_part = "ids=" + ','.join(ids)
    url = "https://api.twitter.com/2/tweets?{}&{}".format(ids_url_part, TWEET_FIELDS)
    return url


def do_work():
    bearer_token = os.environ.get("TWITTER_BEARER_TOKEN")
    headers = create_headers(bearer_token)

    # Use next two lines together to get and delete all rule
    # rules = get_rules(headers)
    # delete_all_rules(headers, rules)
    # set_rules(headers, OUR_RULES)  # Use to set OUR_RULES. Only need to do it once. Can also be done via cURL

    listen_to_stream(headers)


if __name__ == "__main__":
    do_work()
