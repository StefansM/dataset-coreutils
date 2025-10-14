common_files = [
  'query.cpp',
  'query.h',
  'serde.cpp',
  'serde.h',
  'options.h',
  'writer.cpp',
  'writer.h',
  'query_evaluator.cpp',
  'query_evaluator.h',
]

common_deps = [jsondep, boostdep, duckdbdep, arrowdep, arrowdsdep]

cat_exe = executable(
  'dcat',
  'cat.cpp',
  common_files,
  install : true,
  dependencies : common_deps,
)

cut_exe = executable(
  'dcut',
  'cut.cpp',
  common_files,
  install : true,
  dependencies : common_deps,
)

grep_exe = executable(
  'dgrep',
  'grep.cpp',
  common_files,
  install : true,
  dependencies : common_deps,
)

head_exe = executable(
  'dhead',
  'head.cpp',
  common_files,
  install : true,
  dependencies : common_deps,
)

join_exe = executable(
    'djoin',
    'join.cpp',
    common_files,
    install : true,
    dependencies : common_deps,
)

sort_exe = executable(
  'dsort',
  'sort.cpp',
  common_files,
  install : true,
  dependencies : common_deps,
)

sql_exe = executable(
  'dsql',
  'sql.cpp',
  common_files,
  install : true,
  dependencies : common_deps,
)

eval_exe = executable(
  'deval',
  'eval.cpp',
  common_files,
  install : true,
  dependencies : common_deps,
)
