#! /bin/bash
for n in {51..200}; do
    dd if=/dev/urandom of=file$( printf %03d "$n" ).bin bs=1 count=$(( 50000 * 1024 )) &
done
