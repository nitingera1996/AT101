from nltk.sentiment.vader import SentimentIntensityAnalyzer

sia = SentimentIntensityAnalyzer()
POLARITY_THRESHOLD = 0.2


def analyze_sentiment(text, polarity_threshold=POLARITY_THRESHOLD):
    scores = sia.polarity_scores(text)
    pos_score = scores['pos']
    neg_score = scores['neg']
    abs_diff = abs(pos_score - neg_score)
    if abs_diff > polarity_threshold:
        if pos_score > neg_score:
            return pos_score
        else:
            return -1 * neg_score  # Returning as a negative
    return 0
