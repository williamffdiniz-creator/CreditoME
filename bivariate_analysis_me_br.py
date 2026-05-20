# Databricks notebook source
# MAGIC %pip install optbinning

# COMMAND ----------

# DBTITLE 1,Bivariate analysis functions
from optbinning import OptimalBinning
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Union, List, Optional, Tuple, Dict, Any
from scipy import stats
import warnings
warnings.filterwarnings('ignore')

def create_bins(
    df: pd.DataFrame,
    variable: str,
    target: Optional[str] = None,
    method: str = 'auto',
    bins: Optional[Union[int, List[float]]] = None,
    labels: Optional[List[str]] = None,
    optbinning_params: Optional[Dict[str, Any]] = None,
    round_decimals: int = 4
) -> Tuple[pd.Series, Optional[OptimalBinning], Optional[np.ndarray]]:
    """
    Create bins for continuous variables or return categorical variables as-is.
    
    Parameters
    ----------
    df : pd.DataFrame
        Input dataframe
    variable : str
        Variable name to bin
    target : str, optional
        Target variable name (required for optbinning method)
    method : str, default='auto'
        Binning method: 'auto', 'quantile', 'fixed', 'categorical', 'optbinning'
    bins : int or list, optional
        Number of bins or list of bin edges
    labels : list, optional
        Custom labels for bins
    optbinning_params : dict, optional
        Parameters for OptimalBinning (e.g., {'min_bin_size': 0.05, 'max_n_bins': 5})
    round_decimals : int, default=4
        Number of decimal places to round bin edges
    
    Returns
    -------
    tuple
        (binned_series, optbinning_object, rounded_splits)
    """
    series = df[variable].copy()
    optb_object = None
    splits_rounded = None
    
    if method == 'categorical' or series.dtype == 'object' or series.dtype.name == 'category':
        return series.fillna('Missing').astype(str), None, None
    
    if method == 'auto':
        n_unique = series.nunique()
        if n_unique <= 10:
            return series.fillna('Missing').astype(str), None, None
        else:
            method = 'quantile'
            bins = 5
    
    if method == 'optbinning':
        if target is None:
            raise ValueError("Target variable is required for optbinning method")
        
        mask_valid = series.notna() & df[target].notna()
        series_clean = series[mask_valid]
        target_clean = df[target][mask_valid]
        
        default_params = {
            'name': variable,
            'dtype': 'numerical',
            'solver': 'cp',
            'min_bin_size': 0.05,
            'max_n_bins': 5,
            'min_n_bins': 2,
            'monotonic_trend': 'auto'
        }
        
        if optbinning_params:
            default_params.update(optbinning_params)
        
        optb_object = OptimalBinning(**default_params)
        optb_object.fit(series_clean.values, target_clean.values)
        
        splits_original = optb_object.splits
        splits_rounded = np.round(splits_original, round_decimals)
        
        bin_edges = [-np.inf] + list(splits_rounded) + [np.inf]
        
        custom_labels = []
        for i in range(len(bin_edges) - 1):
            lower = bin_edges[i]
            upper = bin_edges[i + 1]
            
            lower_str = '-inf' if lower == -np.inf else f'{lower:.{round_decimals}f}'.rstrip('0').rstrip('.')
            upper_str = 'inf' if upper == np.inf else f'{upper:.{round_decimals}f}'.rstrip('0').rstrip('.')
            
            custom_labels.append(f'[{lower_str}, {upper_str})')
        
        result = pd.Series(index=df.index, dtype='object')
        for i, label in enumerate(custom_labels):
            if i == 0:
                mask = series <= bin_edges[i + 1]
            elif i == len(custom_labels) - 1:
                mask = series > bin_edges[i]
            else:
                mask = (series > bin_edges[i]) & (series <= bin_edges[i + 1])
            
            result[mask & series.notna()] = label
        
        result = result.fillna('Missing')
        return result, optb_object, splits_rounded
    
    mask_missing = series.isna()
    series_clean = series[~mask_missing]
    
    if method == 'quantile':
        if bins is None:
            bins = 5
        binned = pd.qcut(series_clean, q=bins, duplicates='drop')
    elif method == 'fixed':
        if bins is None:
            bins = 5
        binned = pd.cut(series_clean, bins=bins) if isinstance(bins, int) else pd.cut(series_clean, bins=bins)
    else:
        raise ValueError(f"Unknown method '{method}'. Use: 'auto', 'quantile', 'fixed', 'categorical', 'optbinning'")
    
    result = pd.Series(index=df.index, dtype='object')
    result[~mask_missing] = binned.astype(str)
    result[mask_missing] = 'Missing'
    
    return result, None, None


def calculate_woe_iv(
    df: pd.DataFrame,
    binned_var: pd.Series,
    target: str
) -> pd.DataFrame:
    """
    Calculate Weight of Evidence (WoE) and Information Value (IV) for each bin.
    
    Parameters
    ----------
    df : pd.DataFrame
        Input dataframe
    binned_var : pd.Series
        Binned variable
    target : str
        Target variable name (0=Good, 1=Bad)
    
    Returns
    -------
    pd.DataFrame
        DataFrame with WoE, IV, and other metrics for each bin
    """
    temp = pd.DataFrame({'bin': binned_var, 'target': df[target]})
    
    agg = temp.groupby('bin')['target'].agg([('total', 'count'), ('bad', 'sum')]).reset_index()
    agg['good'] = agg['total'] - agg['bad']
    
    total_good = agg['good'].sum()
    total_bad = agg['bad'].sum()
    
    if total_good == 0 or total_bad == 0:
        agg['pct_good'] = 0
        agg['pct_bad'] = 0
        agg['woe'] = 0
        agg['iv_partial'] = 0
    else:
        agg['pct_good'] = agg['good'] / total_good
        agg['pct_bad'] = agg['bad'] / total_bad
        
        agg['pct_good_adj'] = agg['pct_good'].replace(0, 0.0001)
        agg['pct_bad_adj'] = agg['pct_bad'].replace(0, 0.0001)
        agg['woe'] = np.log(agg['pct_good_adj'] / agg['pct_bad_adj'])
        
        agg['iv_partial'] = (agg['pct_good'] - agg['pct_bad']) * agg['woe']
    
    agg['bad_rate'] = agg['bad'] / agg['total']
    agg['good_bad_ratio'] = agg['good'] / agg['bad'].replace(0, 1)
    
    return agg


def create_contingency_table_1(metrics_df: pd.DataFrame) -> pd.DataFrame:
    """
    Create contingency table with row percentages.
    
    Parameters
    ----------
    metrics_df : pd.DataFrame
        Metrics dataframe
    
    Returns
    -------
    pd.DataFrame
        Formatted contingency table
    """
    table = metrics_df[['bin', 'good', 'bad', 'total', 'good_bad_ratio']].copy()
    
    table['pct_good_row'] = (table['good'] / table['total'] * 100).round(2)
    table['pct_bad_row'] = (table['bad'] / table['total'] * 100).round(2)
    table['G/B'] = table['good_bad_ratio'].round(2)
    table['pct_pop'] = (table['total'] / table['total'].sum() * 100).round(2)
    
    result = table[['bin', 'good', 'bad', 'total', 'pct_good_row', 'pct_bad_row', 'pct_pop']].copy()
    result.columns = ['Category', 'Good', 'Bad', 'Total', '% Good', '% Bad', '% Pop.']
    
    return result


def create_contingency_table_2(metrics_df: pd.DataFrame) -> Tuple[pd.DataFrame, float]:
    """
    Create contingency table with column percentages, WoE and IV.
    
    Parameters
    ----------
    metrics_df : pd.DataFrame
        Metrics dataframe
    
    Returns
    -------
    tuple
        (formatted_table, total_iv)
    """
    table = metrics_df[['bin', 'good', 'bad', 'pct_good', 'pct_bad', 'woe', 'iv_partial']].copy()
    
    table['pct_good_col'] = (table['pct_good'] * 100).round(2)
    table['pct_bad_col'] = (table['pct_bad'] * 100).round(2)
    table['WOE'] = table['woe'].round(4)
    table['IV_Partial'] = table['iv_partial'].round(4)
    
    iv_total = table['iv_partial'].sum()
    
    result = table[['bin', 'good', 'bad', 'pct_good_col', 'pct_bad_col', 'WOE', 'IV_Partial']].copy()
    result.columns = ['Category', 'Good', 'Bad', '% Good', '% Bad', 'WOE', 'Partial IV']
    
    return result, iv_total


def calculate_psi(
    df: pd.DataFrame,
    binned_var: pd.Series,
    time_col: str
) -> pd.DataFrame:
    """
    Calculate Population Stability Index (PSI) over time.
    
    Parameters
    ----------
    df : pd.DataFrame
        Input dataframe
    binned_var : pd.Series
        Binned variable
    time_col : str
        Time period column name
    
    Returns
    -------
    pd.DataFrame
        DataFrame with PSI values for each time period
    """
    temp = pd.DataFrame({'bin': binned_var, 'time': df[time_col]})
    temp = temp[temp['bin'] != 'Missing']
    
    time_periods = sorted(temp['time'].unique())
    
    if len(time_periods) < 2:
        return pd.DataFrame()
    
    baseline_period = time_periods[0]
    baseline_dist = temp[temp['time'] == baseline_period]['bin'].value_counts(normalize=True)
    
    psi_results = []
    
    for period in time_periods:
        current_dist = temp[temp['time'] == period]['bin'].value_counts(normalize=True)
        all_bins = set(baseline_dist.index) | set(current_dist.index)
        
        psi_total = 0
        for bin_name in all_bins:
            expected = baseline_dist.get(bin_name, 0.0001)
            actual = current_dist.get(bin_name, 0.0001)
            
            if expected == 0:
                expected = 0.0001
            if actual == 0:
                actual = 0.0001
            
            psi_total += (actual - expected) * np.log(actual / expected)
        
        psi_results.append({'time': period, 'psi': psi_total})
    
    return pd.DataFrame(psi_results)


def plot_stability_over_time(
    df: pd.DataFrame,
    binned_var: pd.Series,
    time_col: str,
    target: str,
    ax: plt.Axes,
    title: str
):
    """
    Plot stability chart (% bad over time).
    
    Parameters
    ----------
    df : pd.DataFrame
        Input dataframe
    binned_var : pd.Series
        Binned variable
    time_col : str
        Time column name
    target : str
        Target variable name
    ax : plt.Axes
        Matplotlib axes object
    title : str
        Chart title
    """
    temp = pd.DataFrame({
        'time': df[time_col],
        'bin': binned_var,
        'target': df[target]
    })
    
    stability = temp.groupby(['time', 'bin'])['target'].agg([
        ('total', 'count'),
        ('bad', 'sum')
    ]).reset_index()
    
    stability['bad_rate'] = (stability['bad'] / stability['total'] * 100)
    pivot = stability.pivot(index='time', columns='bin', values='bad_rate')
    
    colors = plt.cm.viridis(np.linspace(0, 0.9, len(pivot.columns)))
    x_positions = range(len(pivot.index))
    
    for i, col in enumerate(pivot.columns):
        ax.plot(x_positions, pivot[col].values, 
               marker='o', label=col, 
               linewidth=1.5, markersize=4, 
               color=colors[i])
    
    time_labels = [dt.strftime('%Y-%m') for dt in pivot.index]
    
    ax.set_xticks(range(len(time_labels)))
    ax.set_xticklabels(time_labels, rotation=45, ha='center', fontsize=7)
    ax.set_xlim(-0.5, len(pivot.index) - 0.5)
    
    ax.set_xlabel('Time Period', fontsize=11, fontweight='bold')
    ax.set_ylabel('% Bad', fontsize=11, fontweight='bold')
    ax.set_title(title, fontsize=13, fontweight='bold', pad=15)
    ax.legend(title='Category', bbox_to_anchor=(1.02, 1), loc='upper left', fontsize=9)
    ax.grid(True, alpha=0.3, linestyle='--', linewidth=0.5)
    ax.set_facecolor('white')


def plot_population_over_time(
    df: pd.DataFrame,
    binned_var: pd.Series,
    time_col: str,
    ax: plt.Axes,
    title: str
):
    """
    Plot population chart with stacked bars.
    
    Parameters
    ----------
    df : pd.DataFrame
        Input dataframe
    binned_var : pd.Series
        Binned variable
    time_col : str
        Time column name
    ax : plt.Axes
        Matplotlib axes object
    title : str
        Chart title
    """
    temp = pd.DataFrame({
        'time': df[time_col],
        'bin': binned_var
    })
    
    population = temp.groupby(['time', 'bin']).size().reset_index(name='count')
    pivot = population.pivot(index='time', columns='bin', values='count').fillna(0)
    
    colors = plt.cm.viridis(np.linspace(0, 0.9, len(pivot.columns)))
    
    pivot.plot(kind='bar', stacked=True, ax=ax, 
              color=colors, width=0.8, edgecolor='white', linewidth=0.3)
    
    time_labels = [dt.strftime('%Y-%m') for dt in pivot.index]
    
    ax.set_xticks(range(len(time_labels)))
    ax.set_xticklabels(time_labels, rotation=45, ha='center', fontsize=7)
    
    ax.set_xlabel('Time Period', fontsize=11, fontweight='bold')
    ax.set_ylabel('Number of Clients', fontsize=11, fontweight='bold')
    ax.set_title(title, fontsize=13, fontweight='bold', pad=15)
    ax.legend(title='Category', bbox_to_anchor=(1.02, 1), loc='upper left', fontsize=9)
    ax.grid(True, alpha=0.3, axis='y', linestyle='--', linewidth=0.5)
    ax.set_facecolor('white')
    
    plt.tight_layout()


def plot_contingency_table(
    table: pd.DataFrame,
    ax: plt.Axes,
    title: str
):
    """
    Plot contingency table as image.
    
    Parameters
    ----------
    table : pd.DataFrame
        Contingency table
    ax : plt.Axes
        Matplotlib axes object
    title : str
        Table title
    """
    ax.axis('tight')
    ax.axis('off')
    
    tbl = ax.table(
        cellText=table.values,
        colLabels=table.columns,
        cellLoc='center',
        loc='center',
        bbox=[0, 0, 1, 1]
    )
    
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(12)
    tbl.scale(1, 2.2)
    
    n_cols = len(table.columns)
    col_widths = [0.22] + [0.78 / (n_cols - 1)] * (n_cols - 1)
    
    for i in range(len(table.columns)):
        tbl[(0, i)].set_facecolor('#31688e')
        tbl[(0, i)].set_text_props(weight='bold', color='white', fontsize=11)
        tbl[(0, i)].set_edgecolor('white')
        tbl[(0, i)].set_linewidth(1.5)
        tbl[(0, i)].set_width(col_widths[i])
    
    for i in range(1, len(table) + 1):
        for j in range(len(table.columns)):
            if i % 2 == 0:
                tbl[(i, j)].set_facecolor('#F5F5F5')
            else:
                tbl[(i, j)].set_facecolor('#FFFFFF')
            
            tbl[(i, j)].set_edgecolor('#DDDDDD')
            tbl[(i, j)].set_linewidth(0.8)
            tbl[(i, j)].set_width(col_widths[j])
    
    ax.set_title(title, fontsize=13, fontweight='bold', pad=20)


def plot_woe_trend(
    metrics: pd.DataFrame,
    ax: plt.Axes,
    title: str = 'WoE by Bin with Trend Line'
):
    """
    Plot WoE bar chart with linear regression trend line.
    
    Parameters
    ----------
    metrics : pd.DataFrame
        Metrics dataframe with 'bin' and 'woe' columns
    ax : plt.Axes
        Matplotlib axes object
    title : str
        Chart title
    """
    metrics_sorted = metrics.sort_values('bin').reset_index(drop=True)
    x_pos = np.arange(len(metrics_sorted))
    woe_values = metrics_sorted['woe'].values
    
    colors = plt.cm.viridis(np.linspace(0.2, 0.9, len(metrics_sorted)))
    bars = ax.bar(x_pos, woe_values, color=colors, alpha=0.8, edgecolor='black', linewidth=1.2)
    
    slope, intercept, r_value, _, _ = stats.linregress(x_pos, woe_values)
    line_y = slope * x_pos + intercept
    ax.plot(x_pos, line_y, 'r--', linewidth=2.5, label=f'Trend (R²={r_value**2:.3f})', zorder=10)
    
    ax.axhline(y=0, color='black', linestyle='-', linewidth=0.8, alpha=0.5)
    ax.set_xlabel('Bin', fontsize=11, fontweight='bold')
    ax.set_ylabel('Weight of Evidence (WoE)', fontsize=11, fontweight='bold')
    ax.set_title(title, fontsize=12, fontweight='bold', pad=10)
    ax.set_xticks(x_pos)
    ax.set_xticklabels(metrics_sorted['bin'], rotation=0, ha='center', fontsize=9)
    ax.legend(loc='best', fontsize=9)
    ax.grid(axis='y', alpha=0.3, linestyle='--')
    
    for bar, woe in zip(bars, woe_values):
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height, f'{woe:.2f}',
                ha='center', va='bottom' if height > 0 else 'top',
                fontsize=8, fontweight='bold')


def plot_psi_evolution(
    psi_df: pd.DataFrame,
    ax: plt.Axes,
    title: str = 'PSI Over Time'
):
    """
    Plot PSI evolution over time.
    
    Parameters
    ----------
    psi_df : pd.DataFrame
        PSI dataframe with 'time' and 'psi' columns
    ax : plt.Axes
        Matplotlib axes object
    title : str
        Chart title
    """
    if psi_df.empty:
        ax.text(0.5, 0.5, 'Insufficient data to calculate PSI',
                ha='center', va='center', transform=ax.transAxes,
                fontsize=12, color='red')
        ax.set_title(title, fontsize=12, fontweight='bold')
        return
    
    ax.plot(range(len(psi_df)), psi_df['psi'].values, 
            marker='o', linewidth=2.5, markersize=7, color='steelblue',
            label='PSI', markerfacecolor='white', markeredgewidth=2)
    
    ax.set_xlabel('Time Period (index)', fontsize=11, fontweight='bold')
    ax.set_ylabel('PSI', fontsize=11, fontweight='bold')
    ax.set_title(title, fontsize=12, fontweight='bold', pad=10)
    ax.legend(loc='upper right', fontsize=9)
    ax.grid(True, alpha=0.3, linestyle='--')
    
    max_psi = psi_df['psi'].max()
    min_psi = psi_df['psi'].min()
    y_range = max_psi - min_psi
    ax.set_ylim(max(0, min_psi - 0.1 * y_range), max_psi + 0.1 * y_range)


def generate_bivariate_report(
    df: pd.DataFrame,
    variable: str,
    target: str,
    time_col: str,
    method: str = 'auto',
    bins: Optional[Union[int, List[float]]] = None,
    optbinning_params: Optional[Dict[str, Any]] = None,
    round_decimals: int = 4,
    figsize: Tuple[int, int] = (17, 14),
    save_path: Optional[str] = False,
    return_optbinning: bool = False,
    verbose: bool = True
) -> Union[Tuple[plt.Figure, pd.DataFrame, pd.DataFrame, float], 
           Tuple[plt.Figure, pd.DataFrame, pd.DataFrame, float, OptimalBinning, np.ndarray]]:
    """
    Generate comprehensive bivariate analysis report.
    
    Parameters
    ----------
    df : pd.DataFrame
        Input dataframe
    variable : str
        Variable to analyze
    target : str
        Target variable (0=Good, 1=Bad)
    time_col : str
        Time period column name
    method : str, default='auto'
        Binning method: 'auto', 'quantile', 'fixed', 'categorical', 'optbinning'
    bins : int or list, optional
        Number of bins or bin edges
    optbinning_params : dict, optional
        OptimalBinning parameters
    round_decimals : int, default=4
        Decimal places for rounding bin edges
    figsize : tuple, default=(18, 16)
        Figure size (width, height)
    save_path : str, optional
        Path to save the report image
    return_optbinning : bool, default=False
        Whether to return OptimalBinning object
    verbose : bool, default=True
        Whether to print progress information
    
    Returns
    -------
    tuple
        (figure, contingency_table_1, contingency_table_2, iv_total)
        or (figure, table1, table2, iv_total, optbinning_obj, rounded_splits) if return_optbinning=True
    
    Examples
    --------
    >>> fig, tab1, tab2, iv = generate_bivariate_report(
    ...     df=data,
    ...     variable='age',
    ...     target='default',
    ...     time_col='month',
    ...     method='quantile',
    ...     bins=5
    ... )
    
    >>> fig, tab1, tab2, iv, optb, splits = generate_bivariate_report(
    ...     df=data,
    ...     variable='score',
    ...     target='default',
    ...     time_col='month',
    ...     method='optbinning',
    ...     optbinning_params={'min_bin_size': 0.05, 'max_n_bins': 4},
    ...     return_optbinning=True
    ... )
    """
    if verbose:
        print(f"Analyzing variable: {variable}")
        print(f"Target: {target}")
        print(f"Time column: {time_col}")
        print(f"Binning method: {method}")
        if method == 'optbinning':
            print(f"Rounding: {round_decimals} decimal places")
        print("-" * 60)
    
    binned_var, optb_object, splits_rounded = create_bins(
        df, variable, 
        target=target,
        method=method, 
        bins=bins,
        optbinning_params=optbinning_params,
        round_decimals=round_decimals
    )
    
    metrics = calculate_woe_iv(df, binned_var, target)
    
    if verbose:
        print(f"{len(metrics)} categories identified")
        if method == 'optbinning' and optb_object is not None:
            print(f"OptimalBinning status: {optb_object.status}")
            print(f"Original splits: {optb_object.splits}")
            print(f"Rounded splits (USED): {splits_rounded}")
    
    table1 = create_contingency_table_1(metrics)
    table2, iv_total = create_contingency_table_2(metrics)
    psi_df = calculate_psi(df, binned_var, time_col)
    
    if verbose:
        print(f"Information Value (IV): {iv_total:.4f}")
    
    if iv_total < 0.02:
        iv_interpretation = "No predictive power"
    elif iv_total < 0.1:
        iv_interpretation = "Weak predictive power"
    elif iv_total < 0.3:
        iv_interpretation = "Medium predictive power"
    elif iv_total < 0.5:
        iv_interpretation = "Strong predictive power"
    else:
        iv_interpretation = "Very strong predictive power"
    
    if verbose:
        print(f"Interpretation: {iv_interpretation}")
        print("-" * 60)
    
    fig = plt.figure(figsize=figsize, dpi=150)
    gs = fig.add_gridspec(4, 2, hspace=0.55, wspace=0.15, height_ratios=[1.1, 1.1, 1.3, 1.4])
    
    method_label = f" [{method}]" if method == 'optbinning' else ""
    fig.suptitle(
        f'Bivariate Analysis{method_label}: {variable} vs {target}\nIV = {iv_total:.4f} ({iv_interpretation})',
        fontsize=16, fontweight='bold', y=0.98
    )
    
    ax1 = fig.add_subplot(gs[0, 0])
    plot_contingency_table(table1, ax1, 'Contingency Table (% by Row)')
    
    ax2 = fig.add_subplot(gs[0, 1])
    plot_contingency_table(table2, ax2, f'Contingency Table (% by Column) - IV Total: {iv_total:.4f}')
    
    ax3 = fig.add_subplot(gs[1, :])
    plot_stability_over_time(df, binned_var, time_col, target, ax3, 'Stability: % Bad Over Time')
    
    ax4 = fig.add_subplot(gs[2, :])
    plot_population_over_time(df, binned_var, time_col, ax4, 'Population: Number of Clients Over Time')
    
    ax5 = fig.add_subplot(gs[3, 0])
    plot_woe_trend(metrics, ax5, 'WoE by Bin with Trend Line')
    
    ax6 = fig.add_subplot(gs[3, 1])
    plot_psi_evolution(psi_df, ax6, 'PSI Over Time')
    
    plt.tight_layout()
    
    if save_path:
        fig.savefig(save_path, dpi=300, bbox_inches='tight')
        if verbose:
            print(f"Report saved to: {save_path}")
    
    if return_optbinning and optb_object is not None:
        return fig, table1, table2, iv_total, optb_object, splits_rounded
    else:
        return fig, table1, table2, iv_total

# COMMAND ----------

# Dados
# Completos: 2017-01 até 2025-12
# Considerados para análise: 2022-01 2025-09
# In Time: 2022-01 à 2023-12 (24 meses)
# OOT: 2024-01 a 2025-09 (21 meses)
# target: percent7mob1

target = 'target_percent7mob1'
oot_cut = '2024-01-01'

query = """
 SELECT *,
        id_customer AS cod_pessoa,
        adjusted_score AS score_ajustado,
        reference_month as safra,
        CASE 
        WHEN adjusted_score <=0.02 THEN '1-LOW'
        WHEN adjusted_score <= 0.3 THEN '2-MEDIUM'
        ELSE '3-HIGH'
        END AS score_band,
        IF(reference_month >= '2024-01-01', 1, 0) AS oot
   FROM ds_catalog_dev.credit_engine.monitoring_me_br
  WHERE target_percent7mob1 IS NOT NULL
    AND reference_month >= '2022-01-01'
"""

df = spark.sql(query).toPandas()
# SELECT * + aliases criam colunas duplicadas (score_band, safra, cod_pessoa...).
# Manter a ultima ocorrencia (alias do CASE/expressao) e descartar a original.
df = df.loc[:, ~df.columns.duplicated(keep='last')]

# COMMAND ----------

# Check data shape and basic info
print(f"Dataset shape: {df.shape}")
print(f"\nNumber of unique clients: {df['cod_pessoa'].nunique()}")
print(f"\nTarget distribution:")
print(df['target_percent7mob1'].value_counts())
print(f"\nScore statistics:")
print(df['score_ajustado'].describe())
print(f"\nMissing values in key columns:")
print(df[['score_ajustado', 'target_percent7mob1', 'safra']].isnull().sum())

# COMMAND ----------

df['target_percent7mob1'].mean()

# COMMAND ----------

df_analysis = df.copy()
df_analysis['safra'] = pd.to_datetime(df_analysis['safra'])

fig, table1, table2, iv = generate_bivariate_report(
    df=df_analysis,
    variable='score_band',
    target='target_percent7mob1',
    time_col='safra',
    method='categorical',
    round_decimals=4,
    verbose=False
)

# COMMAND ----------

fig_opt, t1_opt, t2_opt, iv_opt, optb, splits = generate_bivariate_report(
    df=df_analysis,
    variable='score_ajustado',
    target=target,
    time_col='safra',
    method='optbinning',
    optbinning_params={'min_bin_size': 0.05, 'max_n_bins':3},
    round_decimals=4,
    return_optbinning=True,
    verbose=True
)

print(f"\n{'═'*60}")
print(f"  CORTES ÓTIMOS (OptBinning)")
print(f"{'═'*60}")
print(f"  Splits encontrados: {splits}")
print(f"  IV OptBinning:      {iv_opt:.4f}")
print(f"  IV Cortes atuais:   {iv:.4f}")
print(f"  Delta IV:           {iv_opt - iv:+.4f}")

# COMMAND ----------

# DBTITLE 1,KS, AUC, and Gini Analysis Functions
from sklearn.metrics import roc_curve, auc, roc_auc_score
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from typing import Tuple, Dict

def calculate_ks_statistic(y_true: np.ndarray, y_score: np.ndarray) -> Tuple[float, float, pd.DataFrame]:
    """
    Calculate Kolmogorov-Smirnov statistic.
    
    Parameters
    ----------
    y_true : np.ndarray
        True binary labels (0=Good, 1=Bad)
    y_score : np.ndarray
        Predicted scores/probabilities
    
    Returns
    -------
    tuple
        (ks_statistic, ks_cutoff, ks_table)
    """
    # Ensure inputs are numpy arrays
    y_true = np.asarray(y_true).flatten()
    y_score = np.asarray(y_score).flatten()
    
    # Create dataframe and sort by score descending
    df_ks = pd.DataFrame({
        'score': y_score,
        'target': y_true
    }).sort_values('score', ascending=False).reset_index(drop=True)
    
    # Calculate cumulative counts
    df_ks['bad'] = df_ks['target']
    df_ks['good'] = 1 - df_ks['target']
    
    total_bad = df_ks['bad'].sum()
    total_good = df_ks['good'].sum()
    
    # Calculate cumulative percentages
    df_ks['cum_bad'] = df_ks['bad'].cumsum()
    df_ks['cum_good'] = df_ks['good'].cumsum()
    df_ks['cum_bad_pct'] = df_ks['cum_bad'] / total_bad
    df_ks['cum_good_pct'] = df_ks['cum_good'] / total_good
    
    # Calculate KS statistic
    df_ks['ks'] = df_ks['cum_bad_pct'] - df_ks['cum_good_pct']
    
    # Find maximum KS
    ks_statistic = df_ks['ks'].max()
    ks_idx = df_ks['ks'].idxmax()
    ks_cutoff = df_ks.loc[ks_idx, 'score']
    
    return ks_statistic, ks_cutoff, df_ks


def calculate_auc_gini(y_true: np.ndarray, y_score: np.ndarray) -> Tuple[float, float]:
    """
    Calculate AUC and Gini coefficient.
    
    Parameters
    ----------
    y_true : np.ndarray
        True binary labels (0=Good, 1=Bad)
    y_score : np.ndarray
        Predicted scores/probabilities
    
    Returns
    -------
    tuple
        (auc_score, gini_coefficient)
    """
    # Ensure inputs are numpy arrays
    y_true = np.asarray(y_true).flatten()
    y_score = np.asarray(y_score).flatten()
    
    auc_score = roc_auc_score(y_true, y_score)
    gini = 2 * auc_score - 1
    
    return auc_score, gini


def plot_ks_curve(y_true: np.ndarray, y_score: np.ndarray, ax: plt.Axes, 
                  title: str, ks_stat: float = None):
    """
    Plot KS curve showing cumulative distributions.
    
    Parameters
    ----------
    y_true : np.ndarray
        True binary labels
    y_score : np.ndarray
        Predicted scores
    ax : plt.Axes
        Matplotlib axes object
    title : str
        Plot title
    ks_stat : float, optional
        KS statistic value to display
    """
    # Ensure inputs are numpy arrays
    y_true = np.asarray(y_true).flatten()
    y_score = np.asarray(y_score).flatten()
    
    # Calculate KS table
    ks_stat_calc, ks_cutoff, df_ks = calculate_ks_statistic(y_true, y_score)
    
    if ks_stat is None:
        ks_stat = ks_stat_calc
    
    # Create population percentage axis
    population_pct = np.linspace(0, 100, len(df_ks))
    
    # Plot cumulative bad (red line)
    ax.plot(population_pct, df_ks['cum_bad_pct'].values * 100, 
            color='red', linewidth=2, label='Cumulative Bad (Default)')
    
    # Plot cumulative good (green line)
    ax.plot(population_pct, df_ks['cum_good_pct'].values * 100, 
            color='green', linewidth=2, label='Cumulative Good (Non-Default)')
    
    # Plot KS line (blue dashed)
    ks_idx = df_ks['ks'].idxmax()
    ks_pop_pct = (ks_idx / len(df_ks)) * 100
    ks_bad_pct = df_ks.loc[ks_idx, 'cum_bad_pct'] * 100
    ks_good_pct = df_ks.loc[ks_idx, 'cum_good_pct'] * 100
    
    ax.plot([ks_pop_pct, ks_pop_pct], [ks_good_pct, ks_bad_pct], 
            color='blue', linestyle='--', linewidth=2, label=f'KS = {ks_stat:.4f}')
    
    # Formatting
    ax.set_xlabel('Population %', fontsize=11, fontweight='bold')
    ax.set_ylabel('Cumulative %', fontsize=11, fontweight='bold')
    ax.set_title(title, fontsize=12, fontweight='bold', pad=10)
    ax.legend(loc='upper left', fontsize=9)
    ax.grid(True, alpha=0.3, linestyle='--')
    ax.set_xlim(0, 100)
    ax.set_ylim(0, 100)
    ax.set_facecolor('white')


def plot_roc_curve(y_true: np.ndarray, y_score: np.ndarray, ax: plt.Axes, 
                   title: str, auc_score: float = None):
    """
    Plot ROC curve for AUC visualization.
    
    Parameters
    ----------
    y_true : np.ndarray
        True binary labels
    y_score : np.ndarray
        Predicted scores
    ax : plt.Axes
        Matplotlib axes object
    title : str
        Plot title
    auc_score : float, optional
        AUC score to display
    """
    # Ensure inputs are numpy arrays
    y_true = np.asarray(y_true).flatten()
    y_score = np.asarray(y_score).flatten()
    
    # Calculate ROC curve
    fpr, tpr, thresholds = roc_curve(y_true, y_score)
    
    if auc_score is None:
        auc_score = auc(fpr, tpr)
    
    # Plot ROC curve
    ax.plot(fpr, tpr, color='darkorange', linewidth=2, 
            label=f'ROC curve (AUC = {auc_score:.4f})')
    
    # Plot diagonal reference line
    ax.plot([0, 1], [0, 1], color='navy', linewidth=2, linestyle='--', 
            label='Random classifier')
    
    # Formatting
    ax.set_xlabel('False Positive Rate', fontsize=11, fontweight='bold')
    ax.set_ylabel('True Positive Rate', fontsize=11, fontweight='bold')
    ax.set_title(title, fontsize=12, fontweight='bold', pad=10)
    ax.legend(loc='lower right', fontsize=9)
    ax.grid(True, alpha=0.3, linestyle='--')
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.set_facecolor('white')


def analyze_model_performance(
    df: pd.DataFrame,
    target_col: str,
    score_col: str,
    cutoff_date: str,
    date_col: str = 'safra',
    figsize: Tuple[int, int] = (16, 10)
) -> Tuple[plt.Figure, pd.DataFrame]:
    """
    Analyze model performance with KS, AUC, and Gini for in-time and out-of-time folds.
    
    Parameters
    ----------
    df : pd.DataFrame
        Input dataframe
    target_col : str
        Target variable column name (0=Good, 1=Bad)
    score_col : str
        Score/probability column name
    cutoff_date : str
        Date to split in-time and out-of-time in format 'YYYY-MM-DD'
    date_col : str, default='safra'
        Date column name
    figsize : tuple, default=(16, 10)
        Figure size (width, height)
    
    Returns
    -------
    tuple
        (figure, metrics_dataframe)
    
    Examples
    --------
    >>> fig, metrics = analyze_model_performance(
    ...     df=df,
    ...     target_col='percent7mob1',
    ...     score_col='score_ajustado',
    ...     cutoff_date='2024-01-01'
    ... )
    """
    # Convert date column to datetime if needed
    df_work = df.copy()
    if df_work[date_col].dtype == 'object':
        df_work[date_col] = pd.to_datetime(df_work[date_col])
    
    cutoff_dt = pd.to_datetime(cutoff_date)
    
    # Split data into in-time and out-of-time
    df_intime = df_work[df_work[date_col] < cutoff_dt].copy()
    df_oot = df_work[df_work[date_col] >= cutoff_dt].copy()
    
    print(f"In-Time samples: {len(df_intime)}")
    print(f"Out-of-Time samples: {len(df_oot)}")
    print("-" * 60)
    
    # Calculate metrics for in-time - ensure proper numpy array conversion
    y_true_intime = df_intime[target_col].values.astype(float)
    y_score_intime = df_intime[score_col].values.astype(float)
    
    ks_intime, ks_cutoff_intime, _ = calculate_ks_statistic(y_true_intime, y_score_intime)
    auc_intime, gini_intime = calculate_auc_gini(y_true_intime, y_score_intime)
    
    # Calculate metrics for out-of-time - ensure proper numpy array conversion
    y_true_oot = df_oot[target_col].values.astype(float)
    y_score_oot = df_oot[score_col].values.astype(float)
    
    ks_oot, ks_cutoff_oot, _ = calculate_ks_statistic(y_true_oot, y_score_oot)
    auc_oot, gini_oot = calculate_auc_gini(y_true_oot, y_score_oot)
    
    # Create metrics summary table
    metrics_df = pd.DataFrame({
        'Fold': ['In-Time', 'Out-of-Time'],
        'KS': [ks_intime, ks_oot],
        'AUC': [auc_intime, auc_oot],
        'Gini': [gini_intime, gini_oot],
        'Samples': [len(df_intime), len(df_oot)],
        'Bad Rate (%)': [
            (y_true_intime.sum() / len(y_true_intime) * 100),
            (y_true_oot.sum() / len(y_true_oot) * 100)
        ]
    })
    
    print("\nModel Performance Metrics:")
    print(metrics_df.to_string(index=False))
    print("-" * 60)
    
    # Create visualization
    fig = plt.figure(figsize=figsize, dpi=120)
    gs = fig.add_gridspec(3, 2, hspace=0.35, wspace=0.25, height_ratios=[1, 1, 0.4])
    
    fig.suptitle(
        f'Model Performance Analysis: {score_col} vs {target_col}\nCutoff Date: {cutoff_date}',
        fontsize=14, fontweight='bold', y=0.98
    )
    
    # Row 1: KS Curves
    ax1 = fig.add_subplot(gs[0, 0])
    plot_ks_curve(y_true_intime, y_score_intime, ax1, 
                  f'KS Curve - Training Set (KS={ks_intime:.4f})', ks_intime)
    
    ax2 = fig.add_subplot(gs[0, 1])
    plot_ks_curve(y_true_oot, y_score_oot, ax2, 
                  f'KS Curve - Test Set (KS={ks_oot:.4f})', ks_oot)
    
    # Row 2: ROC Curves
    ax3 = fig.add_subplot(gs[1, 0])
    plot_roc_curve(y_true_intime, y_score_intime, ax3, 
                   f'ROC Curve - Training Set (AUC={auc_intime:.4f})', auc_intime)
    
    ax4 = fig.add_subplot(gs[1, 1])
    plot_roc_curve(y_true_oot, y_score_oot, ax4, 
                   f'ROC Curve - Test Set (AUC={auc_oot:.4f})', auc_oot)
    
    # Row 3: Metrics Table
    ax5 = fig.add_subplot(gs[2, :])
    ax5.axis('tight')
    ax5.axis('off')
    
    # Format metrics for display
    metrics_display = metrics_df.copy()
    metrics_display['KS'] = metrics_display['KS'].apply(lambda x: f'{x:.4f}')
    metrics_display['AUC'] = metrics_display['AUC'].apply(lambda x: f'{x:.4f}')
    metrics_display['Gini'] = metrics_display['Gini'].apply(lambda x: f'{x:.4f}')
    metrics_display['Bad Rate (%)'] = metrics_display['Bad Rate (%)'].apply(lambda x: f'{x:.2f}%')
    
    table = ax5.table(
        cellText=metrics_display.values,
        colLabels=metrics_display.columns,
        cellLoc='center',
        loc='center',
        bbox=[0, 0, 1, 1]
    )
    
    table.auto_set_font_size(False)
    table.set_fontsize(11)
    table.scale(1, 2.5)
    
    # Style header
    for i in range(len(metrics_display.columns)):
        table[(0, i)].set_facecolor('#31688e')
        table[(0, i)].set_text_props(weight='bold', color='white', fontsize=12)
        table[(0, i)].set_edgecolor('white')
        table[(0, i)].set_linewidth(1.5)
    
    # Style rows
    for i in range(1, len(metrics_display) + 1):
        for j in range(len(metrics_display.columns)):
            if i % 2 == 0:
                table[(i, j)].set_facecolor('#F5F5F5')
            else:
                table[(i, j)].set_facecolor('#FFFFFF')
            table[(i, j)].set_edgecolor('#DDDDDD')
            table[(i, j)].set_linewidth(0.8)
    
    ax5.set_title('Performance Metrics Summary', fontsize=13, fontweight='bold', pad=15)
    
    plt.tight_layout()
    
    return fig, metrics_df

# COMMAND ----------

# Execute the model performance analysis
fig_performance, metrics_performance = analyze_model_performance(
    df=df,
    target_col='target_percent7mob1',
    score_col='score_ajustado',
    cutoff_date='2024-01-01',
    date_col='safra',
    figsize=(16, 10)
)

plt.show()

# Display metrics table
print("\n" + "="*60)
print("FINAL METRICS SUMMARY")
print("="*60)
print(metrics_performance.to_string(index=False))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   reference_month,
# MAGIC   COUNT(*)                                                          AS total,
# MAGIC   SUM(target_percent7mob1)                                          AS total_bad,
# MAGIC   ROUND(SUM(target_percent7mob1) * 100.0 / COUNT(*), 2)            AS bad_rate_pct,
# MAGIC   ROUND(SUM(target_overdue_1m) / NULLIF(SUM(target_billed_1m), 0) * 100, 2) AS overdue_pct_1m
# MAGIC FROM ds_catalog_dev.credit_engine.monitoring_me_br
# MAGIC WHERE target_percent7mob1 IS NOT NULL
# MAGIC   AND reference_month >= '2022-01-01'
# MAGIC GROUP BY reference_month
# MAGIC ORDER BY reference_month

# COMMAND ----------

# MAGIC %sql
# MAGIC  SELECT id_customer AS cod_pessoa,
# MAGIC         adjusted_score AS score_ajustado,
# MAGIC         reference_month as safra,
# MAGIC         --[0.02   0.3993]
# MAGIC         CASE 
# MAGIC         WHEN adjusted_score <=0.02 THEN '1-LOW'
# MAGIC         WHEN adjusted_score <= 0.3993 THEN '2-MEDIUM'
# MAGIC         ELSE '3-HIGH'
# MAGIC         END AS score_band,
# MAGIC         CASE
# MAGIC         WHEN adjusted_score <=0.01 THEN '1-LOW'
# MAGIC         WHEN adjusted_score <= 0.3 THEN '2-MEDIUM'
# MAGIC         ELSE '3-HIGH'
# MAGIC         END AS score_band2,
# MAGIC         target_percent7mob1,
# MAGIC         overdue_pct,
# MAGIC         months_with_billing,	
# MAGIC         months_defaulted,
# MAGIC         pct_months_overdue_10_20 AS 10_20,	
# MAGIC         pct_months_overdue_20_30 AS 20_30,	
# MAGIC         pct_months_overdue_30_50 AS 30_50,	
# MAGIC         pct_months_overdue_50_plus AS 50_plus
# MAGIC    FROM ds_catalog_dev.credit_engine.monitoring_me_br
# MAGIC   --WHERE target_percent7mob1 IS NOT NULL
# MAGIC     WHERE reference_month >= '2022-01-01' and id_customer = '129719'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ds_catalog_dev.credit_engine.monitoring_me_br