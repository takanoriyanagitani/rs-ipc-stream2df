#!/bin/sh

ifile=./sample.d/tmp.file.ipc
istrm=./sample.d/tmp.strm.ipc

gencsv(){
	echo timestamp,severity,status,body
	echo 2025-10-21T01:41:32.012345Z,INFO,200,apt update done
	echo 2025-10-20T01:41:32.012345Z,WARN,500,apt update failure
	echo 2025-10-19T01:41:32.012345Z,WARN,500,apt update failure
}

geninput(){
	echo generating input file...

	mkdir -p ./sample.d

	gencsv |
		csv2arrow2ipc |
		cat > "${ifile}"

	arrow-file-to-stream "${ifile}" > "${istrm}"
}

test -f "${istrm}" || geninput

csv2sql(){
	cat "${istrm}" |
		./rs-ipc-stream2df \
			--tabname 'sample_table' \
			--sql '
				SELECT
					*
				FROM sample_table
				ORDER BY f1
			' |
		arrow-cat
}

ls2sql(){
	dirents2arrow-ipc-stream . |
		./rs-ipc-stream2df \
			--tabname 'dirents_table' \
			--sql "
				SELECT
					*
				FROM dirents_table
				WHERE filetype == 'dir'
				ORDER BY len
			" |
		rs-arrow-ipc-stream-cat
}

echo example 1: use sql to process the csv
csv2sql

which dirents2arrow-ipc-stream | fgrep -q dirents2arrow-ipc-stream || exec sh -c '
	echo dirents2arrow-ipc-stream missing.
	exit 1
'

which rs-arrow-ipc-stream-cat | fgrep -q rs-arrow-ipc-stream-cat || exec sh -c '
	echo rs-arrow-ipc-stream-cat missing.
	exit 1
'

echo
echo example 2: use sql to process the dirents
ls2sql
