from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from io import open


def get_tweet_tone(text):
    # returns number between -100 and 100
    analyser = SentimentIntensityAnalyzer()
    # apparently vader doesn't need punctuation removal/stopword removal.
    score = analyser.polarity_scores(text)
    return int(round(score["compound"]*100))
