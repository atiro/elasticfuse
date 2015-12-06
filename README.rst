Elasticfuse

    Provides a filesystem interface to elasticsearch, allowing you to browse
    indexes, look at document mappings, and read some sample documents.

Usage

    elasticfuse [elasticsearch_url] [local directory]

    will map the indexes into directories within the local directory that 
    you can browse using normal command line tools (cd, ls); under each
    index there are directories for looking the mapping properties and
    for looking at some sample documents in groups of 10.
