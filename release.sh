#!/bin/sh

## Utility script to release project with a tag
## Usage:
##   ./release.sh <tag-name>

if [ X"$1" = X"" ]; then
	echo "Usage: $0 tag-name"
	echo "Example:"
	echo "\t$0 v0.1.2"
	exit 1
fi

echo "$1"
git commit -m "$1"
git tag -f -a "$1" -m "$1"
git push origin "$1" -f
git push
