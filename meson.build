project('bash-functions', 'cpp',
  version : '0.1',
  default_options : ['warning_level=3',
                     'cpp_std=c++17'])

jsondep = dependency('jsoncpp')
boostdep = dependency('boost', modules : ['program_options'])

cc =  meson.get_compiler('cpp')
duckdbdep = cc.find_library('duckdb')

common_files = [
  'query.h',
  'serde.h',
  'options.h',
]

cat_exe = executable(
  'cat',
  'cat.cpp',
  common_files,
  install : true,
  dependencies : [jsondep, boostdep],
)

cut_exe = executable(
  'cut',
  'cut.cpp',
  common_files,
  install : true,
  dependencies : [jsondep, boostdep],
)

grep_exe = executable(
  'grep',
  'grep.cpp',
  common_files,
  install : true,
  dependencies : [jsondep, boostdep],
)

head_exe = executable(
  'head',
  'head.cpp',
  common_files,
  install : true,
  dependencies : [jsondep, boostdep],
)

eval_exe = executable(
  'eval',
  'eval.cpp',
  common_files,
  install : true,
  dependencies : [jsondep, boostdep, duckdbdep],
)
