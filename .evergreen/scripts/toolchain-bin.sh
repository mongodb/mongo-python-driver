# Echo the mongodbtoolchain's "Current" python bin dir for this platform.
# Sourced by configure-env.sh and install-dependencies.sh. The dir may not
# exist on a given host (e.g. no-toolchain machines); callers must check.
mongodb_toolchain_bin() {
  if [ "Windows_NT" = "${OS:-}" ]; then
    echo "/cygdrive/c/Python/Current/Scripts"
  elif [ "$(uname -s)" = "Darwin" ]; then
    echo "/Library/Frameworks/Python.Framework/Versions/Current/bin"
  else
    echo "/opt/python/Current/bin"
  fi
}
