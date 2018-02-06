#!/usr/bin/env bash
# This script will build the swagger documentation and push it to the gh-pages doc
# For now this should only be run on the master branch

set -eu

unset CDPATH
cd "$( dirname "${BASH_SOURCE[0]}" )/.."

# Environment Variables:
# Args: <remote> (branches|tags) <branch_or_tag_name> <commit_message>
#   remote: The git remote url
#   branch_or_tag: Whether docs should go into the branches or tags subdirectory 
#   branch_or_tag_name: The name of the tag or branch
#   commit_message: The commit message

main() {
    if [ "$#" -ne 4 -o "$1" == "-h" ]; then
        print_usage
    fi

    GIT_REMOTE=$1
    DOCS_SUBDIR=$2
    BRANCH_NAME=$3
    COMMIT_MESSAGE=$4

    # Determine version string
    if [ "$DOCS_SUBDIR" == "branches" ]; then
		COMMIT_REF="$(git rev-parse --short HEAD)"
        DOC_VERSION="$BRANCH_NAME/$COMMIT_REF"
    elif [ "$DOCS_SUBDIR" == "tags" ]; then
        DOC_VERSION="$BRANCH_NAME"
    else
        print_usage
    fi

    # Build documentation
	(
    cd swagger
    npm install
    npm run build -- "--docs-version=$DOC_VERSION"
	)

    # Copy documentation 
    if [ "$BRANCH_NAME" == "master" ]; then
        checkin_master
    else
        checkin_branch "$DOCS_SUBDIR/$BRANCH_NAME"
    fi

    # Cleanup subdirectory
    rm -rf gh-pages/
}

# Print usage and exit
print_usage() {
    echo "Usage: $0 <remote> Branch|Tag <branch_or_tag_name> <commit_message>"
    exit 1
}

# Prune branches in a subdirectory of gh-pages
# subdir: The subdirectory name (branches|tags)
# remote_types: The remote type for ls-remote (head|tags)
prune_branches() {
    subdir=$1
    remote_types=$2
    if [ -d "gh-pages/${subdir}/" ]; then
        (
        cd gh-pages
        for branch_dir in ${subdir}/*; do
            branch_name="$(basename ${branch_dir})"
            branch_exists="$(git ls-remote --${remote_types} ${GIT_REMOTE} ${branch_name} | wc -l)"
            if [ "$branch_exists" -eq 0 ]; then
                echo "Pruning branch: ${branch_name}"
                git rm --quiet -rf "${branch_dir}"
            fi
        done
        )
    fi
}

# Checkin documentation for a single branche
# target_dir: The destination directory (e.g. branches/<branch_name>)
checkin_branch() {
   target_dir=$1
   # We try up to 3 times, sleeping for 3 or 7 seconds between attempts
   for i in 3 7 100; do
       # Allow capture of exit code of subshell
       set +e
        (
        set -e

        # Checkout gh-pages
        rm -rf gh-pages/
        git clone ${GIT_REMOTE} --branch gh-pages --single-branch gh-pages

        # Create target directory and copy files
        mkdir -p "gh-pages/${target_dir}"
        cp -R swagger/build/swagger-ui/* "gh-pages/${target_dir}"

        cd gh-pages
	    if [ "$(git status --porcelain)" ]; then
            # Add files
            git add "${target_dir}*"

            # Add any modified files, and push
            git commit --message "$COMMIT_MESSAGE" 

            # Push to remote repo
            git push --quiet 
        else
            echo "No changes to commit"
        fi
        )
        if [ "$?" -eq "0" ]; then
            # Success case
            break
        elif [ "$i" -lt 100 ]; then
            # Failure case, sleep and retry
            echo "Error pushing branch docs, retrying in $i seconds."
            sleep $i
        else
            # Final failure case
            echo "Could not push branch docs, exiting"
            exit 1
        fi
   done

   set -e
}

# Prune non-existing branches and tags, and check-in master documentation, doing a force-push
checkin_master() {
    (
    # Clone the gh-pages branch and prune any branches that don't exist in remotes
    git clone ${GIT_REMOTE} --branch gh-pages --single-branch gh-pages
    prune_branches branches heads
    prune_branches tags tags

    # Copy currently generated documentation into gh-pages
    cp -R swagger/build/swagger-ui/* gh-pages/
    cd gh-pages/

	if [ "$(git status --porcelain)" ]; then
        # Checkout a new orphan branch
        git checkout --quiet --orphan gh-pages-new
        # Add everything that still exists in this folder
        git add *
        # Commit
        git commit --quiet --message "$COMMIT_MESSAGE"
        # Force push to gh-pages 
        git push --quiet --force --set-upstream origin gh-pages-new:gh-pages
    else
        echo "No changes to commit"
    fi
    )
}

main "$@"

