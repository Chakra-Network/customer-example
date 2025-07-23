import argparse
import os

import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
from openai import OpenAI


def get_tweets_from_snowflake(
    user, password, account, warehouse, database, schema, query
):
    """Connects to Snowflake and fetches tweets based on a query."""
    try:
        conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=account,
            warehouse=warehouse,
            database=database,
            schema=schema,
            ocsp_fail_open=True,
        )
        cursor = conn.cursor()

        cursor.execute(query)
        tweets = [row[0] for row in cursor.fetchall()]
        return tweets
    except Exception as e:
        print(f"Error connecting to Snowflake or fetching data: {e}")
        return []
    finally:
        if "conn" in locals() and not conn.is_closed():
            cursor.close()
            conn.close()


def generate_tweets(openai_key, grounding_tweets, recency_tweets, num_tweets):
    """Generates tweets using the OpenAI API."""
    try:
        client = OpenAI(api_key=openai_key)

        system_prompt = f"""
You are an expert tweet author. Your task is to generate {num_tweets} new, distinct, and diverse tweets.

Notably the following are cringe on twitter and you should avoid them:
- Hashtags
- Emojis 
- Random capitalization

You goal is to be a thought leader on stablecoins, ideally with a research tilt. When you write a tweet, it should be grounded in the recent themes and should not be a vapid generic tweet.

Each tweet must satisfy two conditions:
1. The WRITING STYLE must match the style of the following "grounding" tweets:
---
{" ".join(grounding_tweets)}
---

2. The TOPIC or THEME of each tweet should be inspired by the following "recency" tweets:
---
{" ".join(recency_tweets)}
---

Please provide a numbered list of exactly {num_tweets} tweets. Do not include any other text or preamble.
"""
        
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": f"Please generate {num_tweets} diverse tweets."}
            ],
            max_tokens=150 * num_tweets,
            n=1,
            stop=None,
            temperature=0.8, # Increased temperature slightly for more diversity
        )
        
        raw_content = response.choices[0].message.content.strip()
        
        # Post-process the response to extract individual tweets
        generated_tweets = []
        for line in raw_content.split('\n'):
            line = line.strip()
            if not line:
                continue
            # Remove leading numbers like "1. " or "1) "
            if line[0].isdigit() and (line[1] == '.' or line[1] == ')'):
                line = line[2:].strip()
            generated_tweets.append(line)
            
        return generated_tweets
    except Exception as e:
        print(f"Error calling OpenAI API: {e}")
        return []


def main():
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Generate tweets based on grounding and recency tweets from Snowflake."
    )
    parser.add_argument(
        "--output", help="Output CSV file location.", default="generated_tweets.csv"
    )
    parser.add_argument(
        "--num_tweets", help="Number of tweets to generate.", type=int, default=5
    )
    args = parser.parse_args()

    openai_key = os.environ.get("OPENAI_API_KEY")
    snowflake_user = os.environ.get("SNOWFLAKE_USER")
    snowflake_password = os.environ.get("SNOWFLAKE_PASSWORD")
    snowflake_account = os.environ.get("SNOWFLAKE_ACCOUNT")
    snowflake_warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE")
    snowflake_database = os.environ.get("SNOWFLAKE_DATABASE")
    snowflake_schema = os.environ.get("SNOWFLAKE_SCHEMA")

    if not all(
        [
            openai_key,
            snowflake_user,
            snowflake_password,
            snowflake_account,
            snowflake_warehouse,
            snowflake_database,
            snowflake_schema,
        ]
    ):
        print(
            "Error: Missing required credentials. Please ensure all required variables are set in your .env file."
        )
        return

    # --- Snowflake Queries ---
    grounding_query = f"select t.text from twitter_tweets t join twitter_profiles p on t.poster_id = p.id where p.name = 'SquadsProtocol' or p.name = 'Carlos_0x' or p.name = 'SimkinStepan' ORDER by t.timestamp DESC LIMIT 100"
    recency_query = f"select t.text from twitter_tweets t join twitter_profiles p on t.poster_id = p.id where p.name = 'sytaylor' or p.name = 'chuk_xyz' ORDER by t.timestamp DESC LIMIT 500"
    # ----------------------------------------------------

    print("Fetching grounding tweets...")
    grounding_tweets = get_tweets_from_snowflake(
        snowflake_user,
        snowflake_password,
        snowflake_account,
        snowflake_warehouse,
        snowflake_database,
        snowflake_schema,
        grounding_query,
    )

    print("Fetching recency tweets...")
    recency_tweets = get_tweets_from_snowflake(
        snowflake_user,
        snowflake_password,
        snowflake_account,
        snowflake_warehouse,
        snowflake_database,
        snowflake_schema,
        recency_query,
    )

    if not grounding_tweets or not recency_tweets:
        print("Could not fetch tweets from Snowflake. Aborting.")
        return

    print("Generating new tweets...")
    generated_tweets = generate_tweets(
        openai_key, grounding_tweets, recency_tweets, args.num_tweets
    )

    if generated_tweets:
        df = pd.DataFrame({"generated_tweet": generated_tweets})
        df.to_csv(args.output, index=False)
        print(
            f"Successfully generated {len(generated_tweets)} tweets and saved to {args.output}"
        )


if __name__ == "__main__":
    main()
