import os
from pyspark.sql import SparkSession, DataFrame, Row
from data_paths import *


def from_json_to_delta(sparkSession: SparkSession,
                       json_data_path: str,
                       delta_data_path: str) -> DataFrame:
    print('>>> Writing JSON data to delta.')

    if not os.path.exists(json_data_path):
        raise Exception(f'>>> Please download the data and save it to {json_data_path}.')

    if os.path.exists(delta_data_path):
        print(f'>>> {delta_data_path} already exists, skipping operation.')
    else:
        print(f'>>> Reading JSON data from {json_data_path}...')
        df_from_json = spark.read.format('json').load(json_data_path)

        print(f'>>> Writing data to {delta_data_path}...')
        df_from_json.write.format('delta').save(delta_data_path)
        print(f'>>> Done!')

    delta_df = sparkSession.read.format('delta').load(delta_data_path)
    return delta_df


def prepare_commit_wordcount(gh_df: DataFrame, commit_wordcount_path: str) -> None:
    def commit_word_count(row: Row) -> float:
        if len(row.payload.commits) == 0:
            return 0.0

        word_counts = 0
        for commit in row.payload.commits:
            word_counts += len(commit.message.strip().split(' '))
        return word_counts / len(row.payload.commits)

    print('>>> Preparing commit wordcount data...')
    if os.path.exists(commit_wordcount_path):
        print(f'>>> {commit_wordcount_path} already exists, skipping operation.')
        return

    print('>>> Transforming computing average word count per commit message...')
    avg_wordcount_in_push = (gh_df
                             .filter("type == 'PushEvent'")
                             .select('payload')
                             .rdd.map(lambda row: Row(avg_commit_wordcount=commit_word_count(row)))
                             .filter(lambda row: row.avg_commit_wordcount < 15)  # limiting the word count per commit
                             .toDF(['avg_commit_wordcount'])
                             )
    print(f'>>> Writing results to {commit_wordcount_path}...')
    avg_wordcount_in_push.write.mode('overwrite').format('delta').save(commit_wordcount_path)
    print('>>> Done!')


def prepare_hourly_commit_count(gh_df: DataFrame,
                                sparkSession: SparkSession,
                                hourly_commit_count_path: str) -> None:
    print('>>> Preparing commit wordcount data...')
    if os.path.exists(hourly_commit_count_path):
        print(f'>>> {hourly_commit_count_path} already exists, skipping operation.')
        return

    gh_df.createOrReplaceTempView('events')

    print('>>> Preparing hourly commit count...')
    hcc = sparkSession.sql("""
    SELECT
        repo.name,
        date_trunc('hour', created_at) AS date_hour,
        SUM(payload.size) AS commit_count
    FROM
        events
    WHERE
        type == 'PushEvent'
    GROUP BY
        repo.name,
        date_trunc('hour', created_at)
    """)
    print(f'>>> writing results to {hourly_commit_count_path}')
    hcc.write.mode('overwrite').format('delta').save(hourly_commit_count_path)
    print('>>> Done!')


if __name__ == '__main__':
    spark = (SparkSession.builder
             .master("local[*]")
             .config("spark.jars.packages", "io.delta:delta-core_2.12:0.8.0")
             .config("spark.driver.memory", "6g")
             .getOrCreate())

    df = from_json_to_delta(spark, JSON_PATH, DELTA_PATH)

    prepare_commit_wordcount(df, WORDCOUNT_DATA_PATH)

    prepare_hourly_commit_count(df, spark, HOURLY_COMMIT_COUNT_PATH)
