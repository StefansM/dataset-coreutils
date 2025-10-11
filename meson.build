project('bash-functions', 'cpp',
  version : '0.1',
  default_options : ['warning_level=3',
                     'cpp_std=c++20'])

jsondep = dependency('jsoncpp')
boostdep = dependency('boost', modules : ['program_options'])
arrowdep = dependency('arrow')
arrowdsdep = dependency('arrow-dataset')
parquetdep = dependency('parquet')

cc =  meson.get_compiler('cpp')
duckdbdep = cc.find_library('duckdb')

common_files = [
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
