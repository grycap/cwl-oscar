/usr/local/bin/python /app/cwl-oscar \
  --oscar-endpoint https://oscar-grnet.intertwin.fedcloud.eu \
  --oscar-username oscar \
  --oscar-password XXXXXXXX \
  --service-name clt4 \
  --mount-path /mnt/cwl-oscar4/mount \
  --outdir /mnt/cwl-oscar4/test \
  /app/examples/date.cwl \
  /app/examples/empty_input.json