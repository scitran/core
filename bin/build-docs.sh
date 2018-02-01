#!/usr/bin/env bash
# This script will build the swagger documentation and push it to the gh-pages doc
# For now this should only be run on the master branch

set -eu

unset CDPATH
cd "$( dirname "${BASH_SOURCE[0]}" )/.."

# Environment Variables:
#  GIT_REMOTE: The full URL for the remote repo, including auth token
#  TRAVIS_BUILD_NUMBER: The travis build number
# Args:

# Build documentation
pushd swagger
npm install
npm run build
popd

# Decrypt SSH Key
SSH_KEY_FILE=$(mktemp -u $HOME/.ssh/XXXXX)

openssl aes-256-cbc -K $encrypted_55750ae1fbc7_key -iv $encrypted_55750ae1fbc7_iv -in .github_deploy_key.enc -out "$SSH_KEY_FILE" -d

chmod 600 "$SSH_KEY_FILE" \
	 && printf "%s\n" \
		  "Host github.com" \
		  "  IdentityFile $SSH_KEY_FILE" \
		  "  LogLevel ERROR" >> ~/.ssh/config

# Clone the gh-pages branch into a subdirectory (gh-pages)
git clone ${GIT_REMOTE} --branch gh-pages --single-branch gh-pages

# Copy documentation 
cp -R swagger/build/swagger-ui/* gh-pages/

pushd gh-pages
# Check for changes
if [[ `git status --porcelain` ]]; then
	# Configure git
	git config user.email "travis@travis-ci.org"
	git config user.name "Travis CI"
	git config --global push.default simple

	# Add any modified files, and push
	git add *
	git commit --message "Travis Core Docs Build: ${TRAVIS_BUILD_NUMBER}"

	# Push to remote repo
	git push --quiet 
fi
popd

# Cleanup subdirectory
rm -rf gh-pages/

