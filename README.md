# Coreutils for datasets

When working with tabular datasets, I often use ubiquitous command line tools
like `head`, `cut`, and `grep` to get a basic idea of the data:

```console
$ head -n5 nyc-taxi.csv  \
    | cut -f1-3 -d,
"vendor_id","pickup_at","dropoff_at"
"1",2019-01-01 00:46:40.000000,2019-01-01 00:53:20.000000
"1",2019-01-01 00:59:47.000000,2019-01-01 01:18:59.000000
"2",2018-12-21 13:48:30.000000,2018-12-21 13:52:40.000000
"2",2018-11-28 15:52:25.000000,2018-11-28 15:55:45.000000
```

However, CSV files are a poor choice for datasets in general, and it's much
more common to see use a format like [Parquet] for anything except
lowest-common-denominator data distribution.

This package provides coreutils-like binaries for interacting with Parquet
files. The example above becomes:

```console
$ dcat "'nyc-taxi.parquet'" \
    | dcut -f vendor_id -f pickup_at -f dropoff_at \
    | dhead -n5 \
    | deval
"vendor_id","pickup_at","dropoff_at"
"1",2019-01-01 00:46:40.000000,2019-01-01 00:53:20.000000
"1",2019-01-01 00:59:47.000000,2019-01-01 01:18:59.000000
"2",2018-12-21 13:48:30.000000,2018-12-21 13:52:40.000000
"2",2018-11-28 15:52:25.000000,2018-11-28 15:55:45.000000
"2",2018-11-28 15:56:57.000000,2018-11-28 15:58:33.000000
```

The syntax of the `d` (for dataset) commands does differ from the coreutils
commands, but hopefully it's similar enough that the two pipelines are roughly
equivalent.

## Commands

All of these commands accept the `--help` option to get a more detailed
description of the individual command-line switches. I've specified all
arguments as `--flag/-f`-style options, but the most common arguments can also
be specified as positional arguments.

### `dcat`: specify the dataset

Unlike regular coreutils pipelines, all `d` pipelines must start with a `dcat`
(this might be relaxed in the future, but for now it's easier to have a
guaranteed terminus on each end of the pipeline).

`dcat` accepts one parameter: the dataset to query. This must be given in a
form that [DuckDb] understands when pasted after a `FROM` in a SQL command.
That means that you *must include single quotes* if the SQL command given to
DuckDb would.

Usage (note single quotes in table name are passed through):
```console
$ dcat "'nyc-taxi.parquet'"
$ dcat "parquet_scan('nyc-taxi.parquet')"
```

### `dcut`: specify columns

The `dcut` command is used to specify the columns to include in the output. If
no `dcut` command is given, then all columns are included. Columns can be
passed using `--field` (`-f`), similar to `cut`, or as positional arguments.

```console
$ dcut vendor_id pickup_at dropoff_at
$ dcut -f vendor_id -f pickup_at -f dropoff_at
```

### `dhead`: trim rows

Limit the number of rows returned by the query. Note that if the rows aren't
sorted and multiple files are being read, then the selection of rows is
indeterminate. Positional arguments can be used instead of `--lines` (`-n`).

```console
$ dhead -n5
$ dhead 5
```

### `dsort`: sort output

Sort the results of the query by some fields. Use `--reverse` (`-r`) to reverse
the sort direction. Fields can be specified using `--field` (`-f`) or as
positional arguments.

Reversing the sort direction reverses the sort direction of all fields, similar
to adding `DESC` after every column in an `ORDER BY` clause.

If `dsort` is used alongside `dhead`, note that the sorting takes place *before*
the results are truncated, no matter which order `dsort` and `dhead` appear in
the pipeline.

```console
$ dsort -f vendor_id -f pickup_at
$ dsort -r vendor_id pickup_at
```

### `dgrep`: search output

The `dgrep` command is least like its coreutils equivalent, but that's because
it's much more powerful. Instead of searching through an entire row, `dgrep`
searches individual fields and allows all SQL predicates to be used.

Specify the field to search using `--field` (`-f`) and the value to find using
`--value` (`-v`). Alternatively, these may be given as positional arguments in
the order `field`, `value`.

The default predicate is `LIKE`, but this can be changed using the
`--predicate` (`-p`) option. By default, parameters are treated as being of
unknown types, and [DuckDB] is responsible for working out any typecasts. To
force the value to be interpreted as text or an integer, use `--text` (`-t`) or
`--integer` (`-i`). This will be reworked soon to allow for a wider range of
types. In particular, `TIMESTAMP` types aren't yet supported.


```console
$ dgrep -f vendor_id -v 1
$ dgrep vendor_id 1
$ dgrep -i tip_amount 0
$ dgrep -i tip_amount -p '>' 100
```

### `deval`: evaluate pipeline

If you've tried running any of the commands above individually, you'll have
noticed that they don't print any data. Instad, they write a JSON-encoded query
plan that gets consumed by `deval`. A pipeline does nothing unless `deval` is
invoked.

By default, `deval` will write in CSV format to standard output. To write to
another file, use the `--output` (`-o`) option. To write Parquet, add the
`--parquet` (`-p`) option.

```console
$ dcat "'nyc-taxi.parquet'" | dhead | deval
$ dcat "'nyc-taxi.parquet'" | dhead | deval -o output.csv
$ dcat "'nyc-taxi.parquet'" | dhead | deval -o output.parquet -p
```

## Putting it all together

We can, for example, use the [NYC taxi dataset] (I'm using the Parquet version
kindly hosted at `s3://ursa-labs-taxi-data`) to find people who used taxi
vendor "1" and didn't tip, then show the top few furthest journeys:

```console
$ dcat "'nyc-taxi-data/2018/*/data.parquet'" \
    | dcut vendor_id pickup_at dropoff_at tip_amount trip_distance \
    | dgrep vendor_id 1 \
    | dgrep -i tip_amount 0 \
    | dsort -r trip_distance \
    | dhead \
    | deval
```

## Building

The build system is [Meson]:

```console
$ meson setup builddir
$ meson compile -C builddir
$ meson install -C builddir
```

Prequisites: `arrow`, `duckdb` and `boost` (specifically
`boost::program_options`).

## Next steps

There are a few things I plan to improve:

* Improve the parameter detection of `dcat` so that it can infer certain
  formats and doesn't just pass a raw string that's dumbly interpreted by
  DuckDb.

* Improve type detection in `dgrep`. Types should be inferred at query timeG
  rather than specified in `dgrep`. Predicates can be specified in `dgrep`
  but a placeholder predicate should be passed through so a sensible default
  can be inferred at query time.

* Add support for large partitioned datasets. DuckDb [supports predicate
  pushdown][duckdb-arrow] over Parquet... in Python and R. Weirdly, there isn't
  an equivalent to the `parquet_scan` table function for partitioned datasets,
  or even a C++ API to do it.

* Clean up the build directory and write some tests.

[duckdb-arrow]: https://duckdb.org/2021/12/03/duck-arrow.html

[Parquet]: https://parquet.apache.org/
[DuckDb]: https://duckdb.org/
[NYC taxi dataset]: https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page
[Meson]: https://mesonbuild.com/
