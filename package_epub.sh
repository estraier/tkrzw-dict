#! /bin/bash

set -eux

input="${1}"; shift
output="${1}"; shift

rm -f "${output}"

pushd "${input}"
zip -X0 "../${output}" mimetype
zip -0 "../${output}" META-INF/container.xml OEBPS/package.opf
zip -0 "../${output}" OEBPS/skmap.xml OEBPS/style.css OEBPS/nav.xhtml
zip -6 "../${output}" OEBPS/overview.xhtml $(ls OEBPS/main-*.xhtml | sort)
popd
