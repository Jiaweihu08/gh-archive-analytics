from pyspark.sql import DataFrame, SparkSession, functions as F
import streamlit as st
import matplotlib.pyplot as plt
from math import sqrt
from dataclasses import dataclass

Z = 1.96


@dataclass(frozen=True)
class ToleranceStats:
    sample_mean: float
    sample_size: int
    population_size: int
    theoretical_tol: float
    used_all_data: bool


@dataclass(frozen=True)
class CompareToleranceStats:
    population_mean: float
    sample_mean: float
    error: float
    population_size: int
    sample_size: int
    sample_fraction: float
    target_tolerance: float
    theoretical_tol: float
    used_all_data: bool


def compute_stats(data: DataFrame, col_name: str) -> (float, float, int, float):
    agg_result = data.agg(
        F.mean(col_name).alias('mean'),
        F.stddev(col_name).alias('std'),
        F.count(col_name).alias('size')
    ).collect()[0]
    mean, std, size = agg_result.mean, agg_result.std, agg_result.size
    tol = Z * std / (sqrt(size) * mean)
    return mean, std, size, tol


# def iterative_avg_tolerance(data: DataFrame, tol: float, col_name: str, init_sample_size: int = 60) \
#         -> (DataFrame, str):
#     def tolerance_helper(sample):
#         mean, std, size, theoretical_tol = compute_stats(sample, col_name)
#         next_size = min(max_size, (Z * std / (mean * tol)) ** 2)
#         iter_info.append(f'\t sample size: {size}')
#
#         if size == max_size or theoretical_tol <= tol or next_size <= size:
#             iter_info.append(f'\t theoretical tolerance: {theoretical_tol}, fraction: {size / max_size}', )
#             return sample, size
#
#         next_fraction = next_size / max_size
#         iter_info.append(f'\t next fraction: {next_fraction} -> size: {int(next_size)}')
#         return tolerance_helper(data.sample(next_fraction))
#
#     assert 0.0 < tol < 1.0, 'tolerance should be from range (0, 1)'
#
#     max_size = 40000  # data.count()
#     iter_info = []
#     init_fraction = init_sample_size / max_size
#     final_sample = tolerance_helper(data.sample(init_fraction))
#     return final_sample, '\n'.join(iter_info)


def avg_tolerance_no_iter(
        data: DataFrame,
        tol: float,
        col_name: str,
        data_size: int,
        population_mean: float,
        init_sample_size: int = 60,
        use_all_data: bool = False) -> (DataFrame, ToleranceStats):
    if use_all_data:
        print(f'sample size: {data_size}')
        return data, ToleranceStats(population_mean, data_size, data_size, 0.0, use_all_data)

    init_sample_size = min(init_sample_size, data_size)
    init_sample = data.sample(fraction=init_sample_size / data_size)
    init_mean, init_std, init_size, init_tol = compute_stats(init_sample, col_name)
    next_sample_size = (Z * init_std / (init_mean * tol)) ** 2
    if init_tol <= tol:
        print(f'>>> Returning the first sample, init_tol: {init_tol}')
        print(f'sample size: {init_size}')
        return init_sample, ToleranceStats(init_mean, init_size, data_size, init_tol, use_all_data)

    # if next_sample_size <= init_size or next_sample_size >= data_size:
    if next_sample_size >= data_size:
        print(f'next_sample_size <= init_size: {next_sample_size <= init_size}')
        print(f'next_sample_size > data_size: {next_sample_size > data_size}')
        return avg_tolerance_no_iter(data, tol, col_name, data_size, population_mean, use_all_data=True)

    sample = data.sample(fraction=next_sample_size / data_size)
    mean, std, size, theoretical_tol = compute_stats(sample, col_name)

    if theoretical_tol > tol:
        return avg_tolerance_no_iter(data, tol, col_name, data_size, population_mean, use_all_data=True)
    print(f'sample size: {size}')
    return sample, ToleranceStats(mean, size, data_size, theoretical_tol, use_all_data)


def simple_avg_tolerance(data: DataFrame,
                         tol: float,
                         col_name: str) -> (DataFrame, CompareToleranceStats):
    population_agg = data.agg(
        F.mean(col_name).alias('mean'),
        F.count(col_name).alias('size')
    ).collect()[0]
    population_mean, population_size = population_agg.mean, population_agg.size
    print(f'group size: {population_size}')
    sample, sample_stats = avg_tolerance_no_iter(data=data,
                                                 tol=tol,
                                                 col_name=col_name,
                                                 data_size=population_size,
                                                 population_mean=population_mean,
                                                 init_sample_size=30,
                                                 use_all_data=False)
    return sample, CompareToleranceStats(population_mean=round(population_mean, 5),
                                         sample_mean=round(sample_stats.sample_mean, 5),
                                         error=round(abs(population_mean - sample_stats.sample_mean) / population_mean,
                                                     5),
                                         population_size=population_size,
                                         sample_size=sample_stats.sample_size,
                                         sample_fraction=round(sample_stats.sample_size / population_size, 5),
                                         target_tolerance=tol,
                                         theoretical_tol=round(sample_stats.theoretical_tol, 5),
                                         used_all_data=sample_stats.used_all_data)


def grouped_avg_tolerance(data: DataFrame,
                          tol: float,
                          target_col: str,
                          category_col: str,
                          cat_transformation_func=F.col) -> dict[str, CompareToleranceStats]:
    if category_col not in data.columns or target_col not in data.columns:
        raise Exception(f'target column: {target_col} or category column: {category_col} \
        not found in table columns: {data.columns}')

    group_tol_result = {}
    category_in_rows = (data
                        .select(cat_transformation_func(category_col).alias('cat_col_alias'))
                        .distinct()
                        .collect())
    categories = sorted([row['cat_col_alias'] for row in category_in_rows])[:3]
    for category in categories:
        print(f'>>> hour: {category}')
        group_data = (data
                      .filter(cat_transformation_func(category_col) == category)
                      .select(target_col))
        # plot_group(category, group_data)
        group_sample, group_sample_stats = simple_avg_tolerance(data=group_data,
                                                                tol=tol,
                                                                col_name=target_col)
        group_tol_result[category] = group_sample_stats
    return group_tol_result


def plot_group(cat, data):
    fig, ax = plt.subplots()
    data.toPandas().plot.box(ax=ax)
    ax.set_title(f'Hour: {cat}')
    st.pyplot(fig)
