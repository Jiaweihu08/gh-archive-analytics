from pyspark.sql import SparkSession, DataFrame, functions as F
from streamlit.delta_generator import DeltaGenerator
import pandas as pd
import matplotlib.pyplot as plt
import streamlit as st

from average_tolerance import simple_avg_tolerance, grouped_avg_tolerance
from data_paths import DELTA_S3_URL, WORDCOUNT_S3_URL, HOURLY_COMMIT_S3_URL
from queries import all_queries, Query
# from data_paths import DELTA_PATH as DELTA_S3_URL, \
#     WORDCOUNT_DATA_PATH as WORDCOUNT_S3_URL, \
#     HOURLY_COMMIT_COUNT_PATH as HOURLY_COMMIT_S3_URL


st.set_page_config(layout="wide")


def page_setup() -> (SparkSession, DataFrame):
    with st.spinner('Setting up Spark, please wait for it...'):
        hadoop_deps = ','.join(map(lambda a: 'org.apache.hadoop:hadoop-' + a + ':3.2.2', ['common', 'client', 'aws']))
        deps = 'io.delta:delta-core_2.12:0.8.0,com.amazonaws:aws-java-sdk:1.12.20,' + hadoop_deps
        spark_session = (SparkSession.builder
                         .master("local[*]")
                         # .config('spark.jars.packages', 'io.delta:delta-core_2.12:0.8.0')
                         .config("spark.jars.packages", deps)
                         .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                                 "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")
                         # .config("spark.driver.memory", "6g")
                         .getOrCreate())
        spark_session.sparkContext.setLogLevel('WARN')
    st.title('GitHub Dataset Analysis :sunglasses:!')

    with st.spinner('Reading df, please wait for it...'):
        gh_df = spark_session.read.format('delta').load(DELTA_S3_URL)
        gh_df.createOrReplaceTempView("events")
    return spark_session, gh_df


def read_df(spark_session: SparkSession, query: Query):
    query_result = spark_session.sql(query.query_string).toPandas()
    return query_result


def visualize_query(spark_session: SparkSession,
                    query: Query,
                    st_col: DeltaGenerator = st,
                    n_rows: int = 10) -> None:
    query_title = f"<h2 style='text-align: center'>{query.title}</h2>"
    st_col.markdown(query_title, unsafe_allow_html=True)

    # divide the page in two columns
    # left_col, right_col = st.columns((1, 1))

    pd_df = read_df(spark_session, query)
    df_cols = pd_df.columns

    # display matplotlib graph
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.set_title(query.title)
    ax.set_xlabel(df_cols[1])
    ax.barh(pd_df[df_cols[0]], pd_df[df_cols[1]])
    ax.invert_yaxis()

    st_col.pyplot(fig)

    # add expander
    expander = st_col.expander(label='Query details', expanded=False)
    # display query string
    expander.code(query.query_string, language='SQL')
    # display table
    expander.table(pd_df.set_index(df_cols[0]).head(n_rows))


def tolerance_for_wordcount(spark_session: SparkSession) -> None:
    st.title('Average word count per commit message')
    tol = st.slider(label='Average wordcount tolerance', min_value=0.0, max_value=1.0, value=0.2, step=0.005)

    wordcount_df = spark_session.read.format('delta').load(WORDCOUNT_S3_URL).limit(40000)
    sample, stats = simple_avg_tolerance(wordcount_df, tol, 'avg_commit_wordcount')

    mean_df = pd.DataFrame({'averages': [stats.population_mean,
                                         stats.sample_mean,
                                         stats.error * stats.population_mean],
                            'data_source': ['population',
                                            'sample',
                                            'error']})

    tol_df = pd.DataFrame({'values': [stats.target_tolerance, stats.theoretical_tol],
                           'tolerance': ['target', 'theoretical']})

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, 5))
    mean_df.plot(kind='bar', x='data_source', y='averages', ax=ax1, color='b', alpha=0.5, legend=False)
    tol_df.plot(kind='bar', x='tolerance', y='values', ax=ax2, color='b', alpha=0.5, legend=False)

    tol_info = 'target tolerance: {}, theoretical tolerance: {:.4f}'.format(
        stats.target_tolerance,
        stats.theoretical_tol
    )
    means = 'population mean: {:.4f}, sample mean: {:.4f}, error: {:.4f}'.format(
        stats.population_mean,
        stats.sample_mean,
        stats.error
    )
    sample_inf = 'population size: {}, sample size: {}, sample fraction: {}'.format(
        stats.population_size,
        stats.sample_size,
        stats.sample_fraction
    )

    graph_col, right_col_1, right_col_2 = st.columns((3, 1, 1))
    graph_col.pyplot(fig)

    right_col_1.metric(label='Population size', value=stats.population_size)
    right_col_2.metric(label='Sample size', value=stats.sample_size)

    right_col_1.metric(label='Population mean', value=stats.population_mean)
    right_col_2.metric(label='Sample mean', value=stats.sample_mean)

    right_col_1.metric(label='Target tolerance', value=stats.target_tolerance)
    right_col_2.metric(label='Theoretical tolerance', value=stats.theoretical_tol)

    right_col_1.metric(label='Sample fraction', value=stats.sample_fraction)

    # for md in (tol_info, means, sample_inf):
    #     right_col.markdown(f"<h4 style='text-align: center'>{md}</h4>", unsafe_allow_html=True)


def tolerance_for_commit_count(spark_session: SparkSession) -> None:
    st.title('Average commit count per hour')
    tol = st.slider(label='tolerance', min_value=0.0, max_value=1.0, value=0.2, step=0.005)

    cc_df = spark_session.read.format('delta').load(HOURLY_COMMIT_S3_URL).limit(40000)
    group_tol_stats = grouped_avg_tolerance(cc_df, tol, 'commit_count', 'date_hour', F.hour)

    result_df = pd.DataFrame(group_tol_stats.values())

    result_df['error'] = result_df['error'] * result_df['population_mean']
    result_df['hour'] = range(len(result_df))

    fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(10, 35))

    def plot_results(ax, y_cols, offsets, colors, title):
        width = 0.2
        for (col, offset, c) in zip(y_cols, offsets, colors):
            rect = ax.bar(x=result_df['hour'] + offset * width,
                          height=result_df[col].map(lambda x: round(x, 3)),
                          width=width, color=c, alpha=0.5, label=col)
            # ax.bar_label(rect, padding=3)
        ax.grid(axis='y')
        ax.legend()
        ax.set_title(label=title, pad=3)

    plot_results(ax1,
                 ['population_mean', 'sample_mean', 'error'],
                 [-1, 0, 1],
                 ['r', 'b', 'g'],
                 'Population vs. Sample Avg')
    plot_results(ax2,
                 ['population_size', 'sample_size'],
                 [-0.5, 0.5],
                 ['r', 'b'],
                 'Population vs. Sample Size')
    plot_results(ax3,
                 ['target_tolerance', 'theoretical_tol'],
                 [-0.5, 0.5],
                 ['r', 'b'],
                 'Target vs. Theoretical Tolerance')
    ax3.set_ylim(top=tol + 0.05)

    left_col, right_col = st.columns(2)
    left_col.pyplot(fig)
    right_col.table(result_df)


if __name__ == '__main__':
    spark, df = page_setup()
    num_cols = 2
    cols = st.columns(num_cols)

    # ---------- Query Visualization ----------
    with st.container():
        title_to_query_string = {q.title: q for q in all_queries}
        query_names = list(title_to_query_string.keys())
        selected_query = st.selectbox("Select a query to visualize", query_names)
        with st.spinner('Executing query, please wait for it...'):
            visualize_query(spark, title_to_query_string[selected_query], st)

        # selected_queries = st.multiselect("Select queries to visualize:",
        #                                   query_names,
        #                                   query_names[-4:]
        #                                   )
        # st.text(f"Your selections: {selected_queries}")
        # # number of equal-sized columns to us
        #
        # queries = [title_to_query_string[q_name] for q_name in selected_queries]
        # for i in range(len(queries)):
        #     col = cols[i % num_cols]
        #     with st.spinner('Executing query, please wait for it...'):
        #         visualize_query(spark, queries[i], col)

    # ---------- Tolerance ----------
    # st.title('Tolerance Comparison - Average tolerance')

    # ---------- Tolerance, average commit word count ----------
    # with st.container():
    #     tolerance_for_wordcount(spark)

    # ---------- Tolerance, hourly commit count ----------
    # with st.container():
    #     tolerance_for_commit_count(spark)
