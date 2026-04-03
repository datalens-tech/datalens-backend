# dl-dynconfig

Framework for dynamically loading and storing typed application configuration via pluggable sources. Built-in sources include S3 (with caching), in-memory, and null. Subclass `DynConfig` to define a typed config model, then fetch and store it asynchronously via a configured source.
