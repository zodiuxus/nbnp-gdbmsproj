import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pathlib import Path

sns.set_theme(style="whitegrid")


def save_plot(fig, filename, out_dir="plots"):
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    fig.savefig(Path(out_dir) / filename, bbox_inches="tight")
    plt.close(fig)


def plot_metrics(csv_path="benchmark_results.csv", out_dir="plots"):
    df = pd.read_csv(csv_path)

    # --- Average execution time ---
    fig, ax = plt.subplots(figsize=(8, 5))
    sns.barplot(
        data=df,
        x="db",
        y="time_sec",
        hue="complexity",
        estimator="mean",
        ax=ax
    )
    ax.set_title("Average Query Execution Time")
    ax.set_ylabel("Time (seconds)")
    fig.tight_layout()
    save_plot(fig, "avg_execution_time.png", out_dir)

    # --- Distribution ---
    fig, ax = plt.subplots(figsize=(8, 5))
    sns.boxplot(
        data=df,
        x="complexity",
        y="time_sec",
        hue="db",
        ax=ax
    )
    ax.set_title("Query Time Distribution")
    fig.tight_layout()
    save_plot(fig, "time_distribution.png", out_dir)

    # --- Rows vs time ---
    fig, ax = plt.subplots(figsize=(8, 5))
    sns.scatterplot(
        data=df,
        x="rows",
        y="time_sec",
        hue="db",
        style="complexity",
        s=80,
        ax=ax
    )
    ax.set_title("Result Size vs Execution Time")
    fig.tight_layout()
    save_plot(fig, "rows_vs_time.png", out_dir)

