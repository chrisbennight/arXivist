# arXivist

Sets up an arXiv data mirror inclusive of metadata records and associated pdf's. 

# Components

The streaming harvester requires
  * A lambda running `harvest_lambda.py`
  * An S3 bucket where output is stored
  * A dynamo db table where the last harvest date is stored

A onetime process to back-populate historical data will require
  * A lambda running `untar_lambda.py`
  * A sqs FIFO queue
  * An ec2 instance to run `bulk_export.py`

The onetime process resources can be deleted after the run is complete (~24 hours).

All the lambdas are written to only require the base library and boto3, so don't need any packages/dependencies bundled.

## Roles

  * `harvest_lambda.py` requires dynamo read and update permissions, and s3 list, put, and get 
  * `untar_lambda.py` requires the lambda-sqs policy as well as s3 list, put, and get
  * `bulk_export.py` requires dynamo read and update permissions and s3 list, put, and get  (same as `harvest_lambda.py`)

# The output

Mirrored data will be maintained in an s3 folder like such:

```
.
├── extracted/
│   ├── metadata/
│   │   └── {date}/
│   │       └── {date}.{seqid}.json
│   └── pdf/
│       └── {date}/
│           └── {date}.{seqid}.pdf
├── pdf/
│   └── arXiv_pdf_{date}_{part}.tar
└── status/
    └── pdf/
        └── arXiv_pdf_{date}_{part}.tar.processed
```
`{date}` above is the last two digits of the year and the month integer padded to two digits, i.e. February, 2010 looks like 1002<br />
`{seqid}` is a monotonically increasing integer assigned to papers when they are submitted<br />
`{part}` is  a 3 digit integer that is a partition of all files within a month.  Each tar archive is capped at 512MB, after which a new archive with a larger part number is created<br />

The data that will normally be mirrored is under extracted/.   If a person wants to source the output they can trigger an event / SNS notification on this prefix.

the pdf and status directories are used for internal checkpointing / idempotency.



# The process

There is a batch process for populating the back catalog of pdf's, and a streaming process for populating metadata records and pulling in new pdf's.

## Batch

This is comprised of `bulk_export.py` and `untar_lambda.py`

  * SQS queue (FIFO) should be created.  Tar files to be extracted will be submitted here.
    * Visibility of 15 minutes
    * Content based deduplication is disabled
  * The lambda `untar_lambda.py` will be sourced from this SQS queue
    * Concurrency of 10
    * Python runtime 
    * Memory of 1.5GB
    * Runtime of 15 min  (typically 6 min is sufficient but no reason to cap)
    
Run `bulk_export.py` in a tmux window (or screen or nohup & - it's just a long running process) on an ec2 instance.<br />
This will pull the historical s3 items from the arxiv requester pays bucket into a local bucket and submit them to the sqs queue for untaring<br />
The lambda will untar the files are put the output in the s3 prefix extracted.<br />

It's safe to re-run this if needed as the archives and contents will only be copied if they don't already exist/haven't already been processed

You will now want to run `harvest_lambda.py` on the ec2 box (you can run it as `python3 harvest_lambda.py`).  This will start harvesting the OAI-PMH metadata endpoint.<br />
The first time this harvest runs it will take awhile, and won't checkpoint until complete.  Overnight is generally sufficient.  Run this in tmux or some other similar mechanism like the previous process.<br />

Once done the ec2 box, sqs queue, and untar_lambda.py resources can be deleted.

## Streaming

The streaming component consists of `harvest_lambda.py`

  * The lambda, `harvest_lambda.py` will be started by a cloudwatch timer on a 24 hour timer
    * 1.5 GB of memory
    * Concurrency limit of 1
    * Python runtime
    * Runtime limit of 15 minutes
    
Arxiv delivers updates in a batch once per day, so we only need to harvest once per day.<br />
The lambda stores in dynamo the last time it ran, so after the first bulk harvest will only pull items since the last run.<br />
It will pull the metadata from the OAI-PMH endpoint, and pdf files from the api gateway individually (so no untar, etc. needed)<br />

Arxiv tends to take this endpoint down around 7-9 or so eastern, so it's better to have the 24 hour interval start 12 hours or so off from that range.

# Metadata Example

```
{
  "header": {
    "identifier": "oai:arXiv.org:0901.0001",
    "datestamp": "2015-05-13",
    "setSpec": "physics:quant-ph"
  },
  "metadata": {
    "arXivRaw": {
      "id": "0901.0001",
      "submitter": "Andrei Galiautdinov",
      "version": {
        "date": "Tue, 30 Dec 2008 21:10:08 GMT",
        "size": "19kb"
      },
      "title": "Controlled-NOT logic with nonresonant Josephson phase qubits",
      "authors": "Andrei Galiautdinov (University of Georgia)",
      "categories": "quant-ph",
      "comments": "7 pages, 4 figures",
      "doi": "10.1103/PhysRevA.79.042316",
      "license": "http://arxiv.org/licenses/nonexclusive-distrib/1.0/",
      "abstract": "  We establish theoretical bounds on qubit detuning for high fidelity\ncontrolled-NOT logic gate implementations with weakly coupled Josephson phase\nqubits. It is found that the value of qubit detuning during the entangling\npulses must not exceed 2g for two-step, and g for single-step control\nsequences, where g is the relevant coupling constant.\n"
    }
  }
}
```

# Arxiv info

  * OAI-PMH endpoint (metadata)
    * https://arxiv.org/help/oa
  * Bulk Access options
    * https://arxiv.org/help/bulk_data
  * S3 Access
    * https://arxiv.org/help/bulk_data_s3



