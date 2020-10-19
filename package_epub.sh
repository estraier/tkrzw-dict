#! /bin/bash

set -eux

input="${1}"; shift
output="${1}"; shift

rm -f "${output}"

pushd "${input}"
ls
zip -X0 "../${output}" mimetype
zip -r9 "../${output}" META-INF OEBPS
popd
