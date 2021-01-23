# arXivist

Sets up an arXiv mirror

# How

  * Lambda process on a cloudwatch timer using the arXiv OAI-PMH endpoing to pull new records
  * New records are parsed and associated PDF is downloaded from the  arXiv buyer pays bucket
  * Record and pdf are serialize dand written to an s3 bucket
  * Date of last record processed is stored in dynamodb table.  Date is fend in to next harvest attempt.


