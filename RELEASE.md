# Some notes on PyMongo releases

## Versioning

We follow [semver](https://semver.org/) and [pep-0440](https://www.python.org/dev/peps/pep-0440)
for versioning.

We shoot for a release every few months - that will generally just
increment the middle / minor version number (e.g. `3.5.0` -> `3.6.0`).

Patch releases are reserved for bug fixes (in general no new features or
deprecations) - they only happen in cases where there is a critical bug
in a recently released version, or when a release has no new features or
API changes.

In between releases we add `.devN` to the version number to denote the
version under development. So if we just released `3.6.0`, then the
current dev version might be `3.6.1.dev0` or `3.7.0.dev0`. When we make the
next release we replace all instances of `3.x.x.devN` in the docs with the
new version number.

## Deprecation

Changes should be backwards compatible unless absolutely necessary. When
making API changes the approach is generally to add a deprecation
warning but keeping the existing API functional. Deprecated features can
be removed in a release that changes the major version number.

## Doing a Release

1. PyMongo is tested on Evergreen. Ensure the latest commit are passing
    [CI](https://spruce.mongodb.com/commits/mongo-python-driver) as expected.

2. Check Jira to ensure all the tickets in this version have been
    completed.

3. Make a PR that adds the release notes to `doc/changelog.rst`. Generally just
    summarize/clarify the git log, but you might add some more long form
    notes for big changes.

4. Merge the PR.

5. Clone the source repository in a temporary directory and check out the
   release branch.

6. Update the version number in `pymongo/_version.py`.

7.  Commit the change, e.g. `git add . && git commit -m "BUMP <VERSION>"`

7.  Tag w/ version_number, eg,
    `git tag -a '4.1.0' -m 'BUMP 4.1.0'`.

8. Bump the version number to `<next version>.dev0` in
    `pymongo/_version.py`, commit, push.

9. Push commit / tag, eg `git push && git push --tags`.

10. Pushing a tag will trigger the release process on GitHub Actions
    that will require a member of the team to authorize the deployment.
    Navigate to https://github.com/mongodb/mongo-python-driver/actions/workflows/release-python.yml
    and wait for the publish to complete.

11. Make sure the new version appears on
    `https://pymongo.readthedocs.io/en/stable/`. If the new version does not show
    up automatically, trigger a rebuild of "stable" on https://readthedocs.org/projects/pymongo/builds/.

12. Publish the release version in Jira and add a description of the release, such as a the reason
    or the main feature.

13. Announce the release on the [community forum](https://www.mongodb.com/community/forums/tags/c/announcements/driver-releases/110/python)

14. File a ticket for DOCSP highlighting changes in server version and
    Python version compatibility or the lack thereof, for example https://jira.mongodb.org/browse/DOCSP-34040

15. Create a GitHub Release for the tag using https://github.com/mongodb/mongo-python-driver/releases/new.
    The title should be "PyMongo X.Y.Z", and the description should
    contain a link to the release notes on the the community forum, e.g.
    "Release notes: mongodb.com/community/forums/t/pymongo-4-0-2-released/150457"

16. Wait for automated update PR on conda-forge, e.g.: https://github.com/conda-forge/pymongo-feedstock/pull/81
    Update dependencies if needed.


## Doing a Bug Fix Release

1.  If it is a new branch, first create the release branch and Evergreen project.

- Clone the source repository in a temporary location.

- Create a branch from the tag, e.g. `git checkout -b v4.1 4.1.0`.

- Push the branch, e.g.: `git push origin v4.6`.

- Create a new project in Evergreen for the branch by duplicating the "Mongo Python Driver" project.
    Select the option to create a JIRA ticket for S3 bucket permissions.

- Update the "Display Name", "Branch Name", and "Identifier".

- Attach the project to the repository.

- Wait for the JIRA ticket to be resolved and verify S3 upload capability with a patch release on the
  new project.

2.  Create a PR against the release branch.

3.  Create a release using the "Doing a Release" checklist above, ensuring that you
    check out the appropriate release branch in the source checkout.

4.  Cherry-pick the changelog PR onto the `master` branch.
