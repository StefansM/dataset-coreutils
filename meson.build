project('bash-functions', 'cpp',
  version : '0.1',
  default_options : ['warning_level=3',
                     'cpp_std=c++17'])

jsondep = dependency('jsoncpp')
boostdep = dependency('boost', modules : ['program_options'])
arrowdep = dependency('arrow')
arrowdsdep = dependency('arrow-dataset')
parquetdep = dependency('parquet')

cc =  meson.get_compiler('cpp')
duckdbdep = cc.find_library('duckdb')

common_files = [
  'query.h',
  'serde.h',
  'options.h',
]

cat_exe = executable(
  'dcat',
  'cat.cpp',
  common_files,
  install : true,
  dependencies : [jsondep, boostdep],
)

cut_exe = executable(
  'dcut',
  'cut.cpp',
  common_files,
  install : true,
  dependencies : [jsondep, boostdep],
)

grep_exe = executable(
  'dgrep',
  'grep.cpp',
  common_files,
  install : true,
  dependencies : [jsondep, boostdep],
)

head_exe = executable(
  'dhead',
  'head.cpp',
  common_files,
  install : true,
  dependencies : [jsondep, boostdep],
)

sort_exe = executable(
  'dsort',
  'sort.cpp',
  common_files,
  install : true,
  dependencies : [jsondep, boostdep],
)

eval_exe = executable(
  'deval',
  'eval.cpp',
  common_files,
  install : true,
  dependencies : [jsondep, boostdep, duckdbdep, arrowdep, arrowdsdep],
)
