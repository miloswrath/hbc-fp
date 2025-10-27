  out_file="sizes.txt"
  total_file="total_size.txt"

  > "$out_file"
  total=0

  while read -r f; do
    size=$(curl -sI "https://ftp.cdc.gov/pub/pax_g/$f" \
            | awk '/[Cc]ontent-[Ll]ength/ {gsub("\r",""); print $2; exit}')
    echo "$f $size" >> "$out_file"
    [[ $size =~ ^[0-9]+$ ]] && total=$((total + size))
  done < matches.txt

  awk -v sum="$total" \
      'BEGIN {printf "Total bytes: %d (%.2f GB)\n", sum, sum/1024/1024/1024}' \
      > "$total_file"
