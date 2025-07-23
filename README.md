# Text Generation Example BHey ased on Grounding and Recent Tweets

This script generates high-quality tweets in the style of a given set of "grounding" authors, based on themes from a set of "recency" tweets. It fetches the source tweets from a Snowflake database, uses the OpenAI API for generation, and outputs the results to a CSV file.

## Setup

### 1. Install Dependencies

First, install the required Python libraries using pip:

```bash
pip install -r requirements.txt
```

### 2. Set up Environment Variables

This script requires credentials for both Snowflake and OpenAI. Create a file named `.env` in the project root and add the following, replacing the placeholder values with your actual credentials:

```
OPENAI_API_KEY="your_openai_api_key"
SNOWFLAKE_USER="your_snowflake_username"
SNOWFLAKE_PASSWORD="your_snowflake_password"
SNOWFLAKE_ACCOUNT="your_snowflake_account_identifier"
SNOWFLAKE_WAREHOUSE="your_snowflake_warehouse"
SNOWFLAKE_DATABASE="your_snowflake_database"
SNOWFLAKE_SCHEMA="your_snowflake_schema"
```

## Usage

Run the script from the command line. By default, it will generate 5 tweets and save them to `generated_tweets.csv`.

```bash
python main.py
```

You can customize the output file and the number of tweets to generate using command-line arguments:

```bash
python main.py --output "my_awesome_tweets.csv" --num_tweets 10
```

