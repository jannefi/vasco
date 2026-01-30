# NEOWISER AWS Automation Scripts
This directory contains the three scripts that implement the fully automatic,
per‑CID NEOWISER AWS pipeline. These scripts are designed to run unattended
across PROD and EC2, using S3 as the handshake mechanism.

They replace all previous batch-based AWS scripts.

---

## Contents

- prod_dispatch_cids.sh
- prod_collect_results.sh
- ec2_watch_and_process.sh
- (this README)

All scripts MUST be executed from the project root:
    ~/code/vasco/

Because each script uses relative paths such as:
    ./data/local-cats/...
    ./scripts/...
    ./work/...

---

## Prerequisites

### Shared (PROD + EC2)
- AWS CLI configured with region: us-west-2
- Bucket: s3://janne-vasco-usw2
- S3 prefixes created automatically:
    vasco/handshake/from-prod/
    vasco/handshake/from-ec2/

### PROD prerequisites
- Flat IRSA TAP chunk files:
    ./data/local-cats/tmp/positions/new/positions_chunk_<CID>.csv
- TAP processed CIDs stored as:
    ./data/local-cats/tmp/positions/new/positions<CID>_closest.csv
- Python environment with:
    make_optical_seed_from_TAPchunk.py
- Clean local tree (optical_seeds/ remains empty)

### EC2 prerequisites
- Instance type similar to:
    InstanceId: i-00d87ed8b8ee1e4a9
    InstanceType: r7g.4xlarge (Arm/Graviton)
    IAM profile: neowise-ssm-profile (SSM-enabled, no SSH required)
- Python environment with:
    neowise_s3_sidecar.py
    sidecar_to_closest_chunks.py
- SSM Session Manager used for console access
- No SSH or SCP required
- ./work/ directory is created automatically and deleted per CID

---

## Operation Overview

The pipeline processes each CID independently and asynchronously:

1) PROD sends “seed jobs” → S3
2) EC2 watcher consumes jobs, runs sidecar + formatter, returns results
3) PROD collects results into a central directory

No state grows on EC2.
No batch groups.
No manual babysitting required.

---

## How to Run the Pipeline

### 1. Start EC2 watcher (MUST start first)

From EC2 (SSM console):

    cd ~/code/vasco
    nohup bash scripts/automation/ec2_watch_and_process.sh > ec2_watcher.log 2>&1 &

Check:

    tail -f ec2_watcher.log

You should see “IDLE” messages until jobs appear.

---

### 2. Start PROD dispatcher (sends only CIDs without TAP results)

From PROD:

    cd ~/code/vasco
    bash scripts/automation/prod_dispatch_cids.sh

This scans all positions_chunk_<CID>.csv files and sends only those
that DO NOT have TAP closest files:
    positions<CID>_closest.csv

State is stored in:
    .sent_cids

Dispatcher can be safely stopped and resumed at any time.

---

### 3. Run PROD collector (pulls results from EC2)

On PROD, run periodically:

    cd ~/code/vasco
    bash scripts/automation/prod_collect_results.sh

This pulls each completed runout into:
    ./data/local-cats/tmp/positions/aws_inbox/

Then merges into:
    ./data/local-cats/tmp/positions/aws_compare_out/

State stored in:
    .completed_cids

No --delete is ever used on central results.

---

## Restart and Resume Logic

- Dispatcher resumes from .sent_cids
- EC2 watcher resumes from .processed_cids
- Collector resumes from .completed_cids
- Safe to interrupt any script at any time
- S3 serves as the handshake and persistence layer

---

## EC2 Architecture Reference

Instance example used with this pipeline:
- InstanceId: i-00d87ed8b8ee1e4a9
- InstanceType: r7g.4xlarge (16 cores)
- Arm64 architecture
- IAM Role: neowise-ssm-profile
- Storage: gp3 root volume
- Access via AWS SSM Session Manager console
- Public IP: automatically assigned
- SSH is intentionally disabled
- S3 bucket: janne-vasco-usw2 (regional us-west-2)

---

## Notes

- The ./work directory is fully ephemeral and recreated for each CID.
- State files (.sent_cids, .processed_cids, .completed_cids) must NOT be committed to Git.
- Scripts in this directory are the only AWS automation scripts to keep in the repository.
- Old batch scripts have been retired permanently.

---

# End of README
