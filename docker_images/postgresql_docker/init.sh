#!/bin/bash

seconds=10
# Wait until dvdrental data is loaded
until [[ ${status} == 0 ]]; do
    >&2 echo "Database is unavailable - waiting for it... 😴 ($seconds)"
    pg_restore -U ${POSTGRES_USER} -d ${POSTGRES_DB} ./dvdrental.tar
    status=$?
    sleep 10
    seconds=$(expr $seconds + 10)
done
