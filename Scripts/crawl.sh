DAY_STR=$(date +"%Y-%m-%d")
echo $DAY_STR

$(rm -r "/home/ubuntu/httpdata/ocid/*")
$(wget -O "/home/ubuntu/httpdata/ocid/OCID-diff-cell-export-"$DAY_STR"-T000000.csv.gz" "https://opencellid.org/ocid/downloads?token=pk.1d747aefca776719299e26f04d7d331c&type=diff&file=OCID-diff-cell-export-"$DAY_STR"-T000000.csv.gz")
$(wget -O "/home/ubuntu/httpdata/ocid/cell_towers.csv.gz" "https://opencellid.org/ocid/downloads?token=pk.1d747aefca776719299e26f04d7d331c&type=full&file=cell_towers.csv.gz")

echo "Done!"
