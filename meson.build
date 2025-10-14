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

subdir('src')