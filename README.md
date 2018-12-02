# mapsync

mapsync is simple service that synchronize contents of local folder with S3
bucket in one way (downloads data from S3, nothing is uploaded to S3).
It listens for updates via SQS queue (this should be manually configured) for
S3 events.

After each sync, it creates request to reload game maps, it writes
`rescan_pending 1` into rcon FIFO (this stuff available in my [fork][fork]).

[fork]: https://github.com/TheRegulars/xonplaces/commit/21c92a7459c7789beb77256deb3b4dbfd20dbe3a
