
# Fast single-shot CDS, minimal backoff, no internal blocksize param
export VASCO_CDS_MODE=single
export VASCO_CDS_MAX_RETRIES=2
export VASCO_CDS_BASE_BACKOFF=1.5
export VASCO_CDS_BLOCKSIZE=omit
export VASCO_CDS_INTER_CHUNK_DELAY=0
export VASCO_CDS_JITTER=0
export VASCO_CDS_PRECALL_SLEEP=0
export VASCO_CDS_GAIA_TABLE="I/350/gaiaedr3"
export VASCO_CDS_PS1_TABLE="II/389/ps1_dr2"
export VASCO_SPIKES_ENGINE=vectorized
export VASCO_SPIKES_DEBUG=1

