#!/bin/bash

# Run locally on your machine to copy to dumbo.

SSH_CONF="$HOME/.ssh/nyu_config"
USERNAME='ch1751'
HOST='dumbo'
DESTDIR='bigdata_project/'

cmd="scp -F $SSH_CONF '*' $USERNAME@$HOST:$DESTDIR"

echo $cmd
$cmd
