import pandas as pd
import matplotlib.pyplot as plt


def _fig_pair_attrib(df: pd.DataFrame, top_n_pairs: int):
    """Build a horizontal bar chart of pair attribution.

    The input ``df`` is expected to contain numeric columns representing
    attribution values.  In production this dataframe is built from several
    joins and occasionally ends up with only non-numeric data (for example
    when the dataset is empty or the columns are of type ``object``).  The
    original implementation blindly forwarded the dataframe to
    ``pandas.DataFrame.plot`` which raised ``TypeError: no numeric data to
    plot`` when this happened.

    To make the function robust we now select only numeric columns before
    plotting and gracefully handle the case where there is nothing to plot by
    returning an empty figure.
    """

    fig, ax = plt.subplots()

    # Nothing to do when the incoming dataframe has no rows or columns.
    if df.empty:
        ax.set_axis_off()
        return fig

    # Attempt to convert every column to a numeric type.  ``to_numeric`` with
    # ``errors='coerce'`` will turn anything that looks like a number (including
    # strings such as "1" or "2.5") into a real numeric dtype and replace
    # anything else with ``NaN``.  After coercion, select only the numeric
    # columns and drop those that are entirely ``NaN``.
    numeric_df = (
        df.apply(pd.to_numeric, errors="coerce")
        .select_dtypes(include="number")
        .dropna(axis=1, how="all")
    )

    # If, after coercion, there is still nothing numeric to plot we simply
    # return an empty figure with its axes hidden to avoid ``TypeError: no
    # numeric data to plot`` from pandas.
    if numeric_df.empty:
        ax.set_axis_off()
        return fig

    combined = numeric_df.head(top_n_pairs)

    # ``pandas.DataFrame.plot`` can still raise ``TypeError`` in some edge
    # cases (for example if all columns are categorical despite the above
    # filtering).  Guard against that by catching the error and returning an
    # empty figure instead of blowing up the report generation.
    try:
        combined.plot(kind="barh", ax=ax)
    except TypeError:
        ax.set_axis_off()

    return fig


if __name__ == "__main__":
    # Simple smoke test when run directly
    data = pd.DataFrame({"pair": ["A/B", "C/D"], "value": [1.2, -0.5]})
    data.set_index("pair", inplace=True)
    _fig_pair_attrib(data, 5)
