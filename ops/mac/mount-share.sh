#!/usr/bin/env bash
# Mount the Beelink data share on the MacBook if it isn't already, and verify it.
#
# macOS does not remount SMB shares after a reboot, and the code needs the share
# at MVP_DATA_ROOT (/Volumes/mvp-data). Idempotent: safe to run before any job.
#
#   ops/mac/mount-share.sh
#
# Handles the one trap: a leftover plain directory at the mount point (from a
# job that ran while the share was down) makes the next mount land at
# /Volumes/mvp-data-1, silently pointing every path at an empty stub.
set -uo pipefail

SHARE_URL="smb://happybees@HAPPYBEES-SEI.local/mvp-data"
MOUNT="/Volumes/mvp-data"
PROBE="$MOUNT/aggregate/atptour/matches.parquet"

is_mounted() { mount | grep -q " on $MOUNT (smbfs"; }

if is_mounted && [ -f "$PROBE" ]; then
    echo "share is mounted at $MOUNT"
    exit 0
fi

# Mounted somewhere else (the -1 suffix case): the stub below is the cause.
other=$(mount | grep -E "mvp-data on /Volumes/mvp-data-[0-9]+ " | sed -E 's/.* on (\/Volumes\/[^ ]+) .*/\1/' || true)
if [ -n "$other" ]; then
    echo "share is mounted at $other, not $MOUNT — ejecting it so it can remount at the right path"
    diskutil unmount "$other" >/dev/null || { echo "ERROR: could not unmount $other" >&2; exit 1; }
fi

# A plain directory at the mount point blocks the mount. /Volumes is root-owned.
if [ -d "$MOUNT" ] && ! is_mounted; then
    if [ -z "$(ls -A "$MOUNT")" ]; then
        echo "removing empty stub directory $MOUNT (sudo)"
        sudo rmdir "$MOUNT" || exit 1
    else
        echo "ERROR: $MOUNT exists, is not a mount, and is not empty. Inspect it before continuing." >&2
        exit 1
    fi
fi

echo "mounting $SHARE_URL ..."
# `open` hands the URL to Finder, which creates /Volumes/mvp-data and uses the
# keychain password; mount_smbfs needs a pre-made mount point and can't do that.
open "$SHARE_URL"
for _ in $(seq 1 30); do
    if is_mounted && [ -f "$PROBE" ]; then
        echo "share is mounted at $MOUNT"
        exit 0
    fi
    sleep 1
done

echo "ERROR: share did not come up within 30s." >&2
echo "  Is the Beelink reachable?  ping -c 1 HAPPYBEES-SEI.local" >&2
echo "  Current mvp-data mounts:" >&2
mount | grep mvp-data >&2 || echo "  (none)" >&2
exit 1
