import requests
import os
import json

# TODO: Take user accounts from config and set as rules
OUR_RULES = [
    {"value": "from:elonmusk", "tag": "Elon Musk Tweets"},
    {"value": "from:WuBlockchain", "tag": "Blockchain journalists tweets"},
    {"value": "#bitcoin", "tag": "Bitcoin hashtag"},
]


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
            json_response = json.loads(response_line)
            # TODO : Do sentiment analysis of tweets
            print(json.dumps(json_response, indent=4, sort_keys=True))


def do_work():
    bearer_token = os.environ.get("TWITTER_BEARER_TOKEN")
    headers = create_headers(bearer_token)

    # Use next two lines together to get and delete all rule
    # rules = get_rules(headers)
    # delete_all_rules(headers, rules)

    # Use to set OUR_RULES. Only need to do it once. Can also be done via cURL
    # set_rules(headers, OUR_RULES)

    listen_to_stream(headers)


if __name__ == "__main__":
    do_work()
