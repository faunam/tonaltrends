from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


def get_tweet_tone(tweet):
    # returns number between -100 and 100
    analyser = SentimentIntensityAnalyzer()
    # apparently vader doesn't need punctuation removal/stopword removal.
    score = analyser.polarity_scores(tweet["text"])
    return score["compound"]*100
