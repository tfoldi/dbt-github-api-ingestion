from snowflake.snowpark.functions import call_udf, col

MAX_PAGES = 100
REPOSITORY = "dbt-labs/dbt-core"
GH_TOKEN = '<your_github_token>'

def model(dbt, session):
    ret = None

    for page in range(1, MAX_PAGES):
        starsnow_params = session.create_dataframe(
            [
                [
                    f"https://api.github.com/repos/{REPOSITORY}/commits",
                    {
                        "method": "get",
                        "params": {"page": page, "per_page": 100},
                        # Feel free to comment out the next line in case 
                        # you do not have github token                        
                        "headers": {"authorization": "Bearer " + GH_TOKEN},
                    },
                ]
            ],
            schema=["url", "params"],
        )

        df = starsnow_params.select(
            call_udf("STARSNOW_REQUEST", col("url"), col("params")).as_("response")
        ).select(
            col("response")["data"].as_("github_commits"),
            col("response")["headers"]["link"].as_("link"),
        )

        if not ret:
            ret = df
        else:
            ret = ret.union_all(df)

        if not df.first()["LINK"] or "next" not in df.first()["LINK"]:
            break

    return ret