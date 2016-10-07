#!/usr/bin/env python3.5
import datetime
import logging
import re
import time
from multiprocessing import Process

import praw
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

user_agent = "python-praw:assumed-gender-stat-collector:v0.1 (by /u/jerenept)"
pat = pattern = re.compile(r"did (.+){1,3} just assume .*", re.IGNORECASE)  # ...did [1-3 words] just assume [word]...
metareddits = ("subredditdrama", "bestof", "worstof", "shitredditsays",)  # as I discover other erroneous entries I add
# also I don't know if excluding metas is the right idea... I'll leave this here but comment it
metabots = ("srscreenshot", "ttumblrbots", "totesmessenger", "snapshillbot")
logging.basicConfig(level=logging.INFO)


def main():
    logging.info("Starting monitor of Reddit for statistics")
    collector_processes = []
    for x in range(0, 3):
        pr = Process(target=initial_collect, args=(pat, metabots,))
        logging.info("Started thread {}".format(x))
        pr.start()
        logging.info("Started processing on thread {}".format(x))
        collector_processes.append(pr)
    updater_process = Process(target=update)
    updater_process.start()
    logging.info("Updater process started")
    while True:
        try:
            for x in range(0, 3):
                if not collector_processes[x].is_alive():
                    logging.warning("Process {} has died. Restarting.".format(x))
                    collector_processes[x] = Process(target=initial_collect, args=(pat, metabots,))
                    collector_processes[x].start()
                    logging.info("Process {} restarted".format(x))
            if not updater_process.is_alive():
                logging.warning("Updater process has died. Restarting.")
                updater_process = Process(target=update)
                updater_process.start()
        except KeyboardInterrupt:
            logging.info("Keyboard Interrupt (C-c) received.")
            logging.info("Ending child processes and exiting.")
            for proc in collector_processes:
                proc.terminate()
            updater_process.terminate()
            exit()


def initial_collect(pat, metabots):
    logging.info("Starting Reddit scanning thread")
    mongo = MongoClient()
    db = mongo.assumed_db
    comment_store = db.comment_store
    logging.info("Connected to database and reading from collection in Reddit scanning thread")
    reddit = praw.Reddit(user_agent=user_agent)
    logging.info("Started Reddit connection on Reddit scanning thread")

    while True:  # forever
        try:
            all_comments = reddit.get_comments("all", limit=200)  # get as many comments as we can in a single call
        except praw.errors.RateLimitExceeded as rle:
            logging.error("Rate-Limited by reddit API. Sleeping for {} seconds".format(rle.sleep_time))
            time.sleep(rle.sleep_time)
            continue
        for comment in all_comments:
            # guards against trying to load deleted comments, and loading comments from metabots that quote
            if comment.body and comment.author and pat.search(comment.body) and \
                            comment.author.name.lower() not in metabots:  # we found the "joke" in the comment
                logging.info("Comment found in /r/{} : id {}".format(comment.subreddit.display_name, comment.id))
                commdata = {
                    "_id": comment.id,
                    "author": comment.author.name,
                    "subreddit": comment.subreddit.display_name,
                    "body": comment.body,
                    "datetime": datetime.datetime.utcfromtimestamp(comment.created_utc),
                    "karma_0": comment.ups,
                    "karma_60": None,
                    "karma_24h": None,
                    "permalink": comment.permalink,
                    "deleted": False,
                }
                try:
                    comment_store.insert_one(commdata)
                except DuplicateKeyError:
                    logging.debug("Already added id: {} to the database".format(comment.id))
                    continue
                logging.debug("Comment id:{} saved in database".format(comment.id))


def update():
    """sets the karma_60 and karma_24h attributes. (amount of karma at 1 and 24 hours after making the comment)"""
    logging.info("Starting database scanning thread")
    mongo = MongoClient()
    db = mongo.assumed_db
    comment_store = db.comment_store
    logging.info("Connected to database and reading from collection on database updater thread")
    reddit = praw.Reddit(user_agent=user_agent)
    logging.info("Started Reddit connection on database updater thread")
    while True:
        for comment in comment_store.find(
                {
                    "datetime": {
                        "$lt": datetime.datetime.utcnow() - datetime.timedelta(hours=1),
                    },
                    "karma_60": {
                        "$eq": None,
                    },
                    "deleted": {
                        "$eq": False,
                    }
                }):
            logging.info("Updating Comment id {}: 1 hour".format(comment['_id']))
            try:
                updated_comment = reddit.get_submission(comment['permalink']).comments[0]
            except praw.errors.NotFound:
                comment_store.update({"_id": comment['_id']}, {"$set": {"deleted": True}})
                continue
            if updated_comment.body:
                comment_store.update({"_id": comment['_id']}, {"$set": {"karma_60": updated_comment.ups}})
            else:
                comment_store.update({"_id": comment['_id']}, {"$set": {"deleted": True}})

        for comment in comment_store.find(
                {
                    "datetime": {
                        "$lt": datetime.datetime.utcnow() - datetime.timedelta(hours=24),
                    },
                    "karma_24h": {
                        "$eq": None,
                    },
                    "deleted": {
                        "$eq": False,
                    }
                }):
            logging.info("Updating Comment id {}: 24 hours".format(comment['_id']))
            try:
                updated_comment = reddit.get_submission(comment['permalink']).comments[0]
            except (praw.errors.NotFound, IndexError):
                comment_store.update({"_id": comment['_id']}, {"$set": {"deleted": True}})
                continue
            if updated_comment.body:
                comment_store.update({"_id": comment['_id']}, {"$set": {"karma_24h": updated_comment.ups}})
            else:
                comment_store.update({"_id": comment['_id']}, {"$set": {"deleted": True}})


if __name__ == "__main__":
    main()
