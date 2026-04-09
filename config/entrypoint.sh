#!/bin/bash
# =============================================================================
# entrypoint.sh — Container Entrypoint
# Expands environment variables in msmtprc, then executes the main command.
# =============================================================================
set -e

# Expand env vars in msmtprc template (msmtp does not support native env vars)
if [ -f /etc/msmtprc.template ]; then
    envsubst < /etc/msmtprc.template > /tmp/msmtprc
    # msmtprc must be owned by the calling user and have restricted permissions
    chmod 600 /tmp/msmtprc
    export MSMTPRC=/tmp/msmtprc
fi

# Execute the command passed to the container
exec "$@"
