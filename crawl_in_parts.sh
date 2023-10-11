set -x  # uncomment to debug
<<<<<<< HEAD
# Usage: ./crawl_in_parts.sh ./to_crawl ./data ./log

=======
# Usage:
>>>>>>> 4d87c3c (Add crawler changes)
URL_PARTS_DIR=$1  # all URLS must be in this dir, split into multiple CSVs
OUTDIR=$2  # out data dir
LOGFILE=$3  # out log dir
MOBILE=$4  # leave empty for desktop, -m mobile mobile
METHOD=$5


echo "Will crawl urls in dir: ${URL_PARTS_DIR}"
echo "Output dir: ${OUTDIR}"
echo "Log file: ${LOGFILE}"
echo "Mobile: ${MOBILE}"

mkdir -p $OUTDIR

echo_date(){
  date -u +"%Y-%m-%dT%H:%M:%S.%3NZ"
}

for url_file in $URL_PARTS_DIR/*_*.csv; do
    echo "$(echo_date) Will crawl the urls in $url_file"
    npm run crawl -- -i $url_file  -o $OUTDIR -v -f -q $METHOD >> $LOGFILE
done
