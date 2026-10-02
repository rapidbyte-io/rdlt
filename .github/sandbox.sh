#!/bin/sh
# Makes the sandbox rdlt ships available on a GitHub Ubuntu runner, so that the tests that need
# one run, and, with RDLT_REQUIRE_SANDBOX set, fail rather than skip where it cannot be made.
set -eu

# Bubblewrap from Ubuntu's signed archive, at /usr/bin/bwrap, where the host looks for it.
sudo apt-get update -qq
sudo apt-get install -y -qq --no-install-recommends bubblewrap

# Ubuntu 24.04 lets a program make an unprivileged user namespace only where an AppArmor
# profile names it. This profile names bubblewrap alone, and grants it that and nothing more:
# narrower than turning the restriction off for every program.
if [ -e /proc/sys/kernel/apparmor_restrict_unprivileged_userns ]; then
  sudo tee /etc/apparmor.d/rdlt-bwrap > /dev/null << 'EOF'
abi <abi/4.0>,
include <tunables/global>

profile rdlt-bwrap /usr/bin/bwrap flags=(unconfined) {
  userns,
}
EOF
  sudo apparmor_parser -r /etc/apparmor.d/rdlt-bwrap
fi

# A sandbox as the host makes one: its own user namespace, in which it may make none.
bwrap --unshare-all --unshare-user --disable-userns --die-with-parent --new-session \
  --ro-bind /usr /usr --symlink usr/lib /lib --symlink usr/lib64 /lib64 --symlink usr/bin /bin \
  --proc /proc --dev /dev -- /usr/bin/true
