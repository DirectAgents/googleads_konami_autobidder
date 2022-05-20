import os
import sys
import argparse
import logging

logging.root.setLevel(logging.INFO)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Deploy Docker Image to Azure")
    parser.add_argument(
        "-e",
        "--env",
        type=str,
        required=True,
        help="The Environment of deployment."
    )

    args = parser.parse_args()
    env_names = ("verify", "production")
    if args.env in env_names:
        logging.info(f"Building Docker Image for ENV={args.env}")
        os.system(f"""
        az acr build \
        --image lookback:$(git log -1 --format=%ct) \
        --image lookback:{args.env} \
        --registry directagents \
        .
        """)
    else:
        logging.error(f"Invalid Environment Name \n Valid Environments: {env_names}")
        sys.exit(1)
