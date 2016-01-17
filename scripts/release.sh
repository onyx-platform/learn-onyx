#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset
set -o xtrace

cd "$(dirname "$0")/.."

if [[ "$#" -ne 2 ]]; then
	echo "Usage: $0 core-version release-branch"
	echo "Example: $0 0.8.4 0.8.x"
fi

new_core_version=$1

# Update to release version.
git checkout master
git stash
git pull

lein set-version $new_core_version
lein update-dependency org.onyxplatform/onyx $new_core_version

git add project.clj
git commit -m "Upgrade to $new_core_version."
git push origin master

# Merge artifacts into answers branch
git checkout answers
git pull || true
git merge -m "Merge branch 'master' into answers" master -X theirs
git push -u origin answers
