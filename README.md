ElasticSearch Backup
-------------------------------------

Small utility for backing up ElasticSearch.

```
$ ./bin/esbackup -h

usage: esbackup [-h] [-v] [--host HOST] [--port PORT] [--filePath FILEPATH]
                {pack,unpack} ...

ElasticSearch backup

Optional arguments:
  -h, --help           Show this help message and exit.
  -v, --version        Show program's version number and exit.
  --host HOST          Host location of ElastSearch instance.
  --port PORT          The ElasticSearch is listening on.
  --filePath FILEPATH  Target directory for backup files

commands:
  {pack,unpack}
```

```
$ ./bin/esbackup pack -h

usage: esbackup pack [-h] [--index INDEX] [--type TYPE]

Optional arguments:
  -h, --help     Show this help message and exit.
  --index INDEX  The index to be archived.
  --type TYPE    The type to be archived.
```

```
$ ./bin/esbackup unpack -h

usage: esbackup unpack [-h] [--ver VER]

Optional arguments:
  -h, --help  Show this help message and exit.
  --ver VER   Which ES backup to pull from, defaults to latest
```
