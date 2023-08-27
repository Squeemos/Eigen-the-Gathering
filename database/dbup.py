"""Database update tool."""

import argparse


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("command", choices=["pull", "update", "push"])

    return parser.parse_args()


def pull_db():
    pass


def update_db():
    pass


def push_db():
    pass


def main():
    args = parse_args()
    cmd = args.command

    if cmd == "pull":
        print("dbup: Pulling newest db...")
        pull_db()
    if cmd == "update":
        print("dbup: Downloading data and updating last db...")
        update_db()
    if cmd == "push":
        print("dbup: Pushing newest db...")
        push_db()


if __name__ == "__main__":
    main()
    