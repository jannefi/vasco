
# Fast single-shot CDS, minimal backoff, no internal blocksize param
export VASCO_CDS_MODE=single
export VASCO_CDS_MAX_RETRIES=2
export VASCO_CDS_BASE_BACKOFF=1.5
export VASCO_CDS_BLOCKSIZE=omit
export VASCO_CDS_INTER_CHUNK_DELAY=0
export VASCO_CDS_JITTER=0
export VASCO_CDS_PRECALL_SLEEP=0
