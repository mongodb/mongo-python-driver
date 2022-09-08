

regex="cp3([[:digit:]]+)-.*universal2"

line="pymongo-4.2.0-cp310-cp310-macosx_10_15_universal2.whl"

if [[ "$line" =~ $regex ]]; then
  echo "Match count is ${#BASH_REMATCH[@]}."
  echo ${BASH_REMATCH[1]}
else
  echo "No match."
fi
