#!/bin/bash
# EC2 cloud-init bootstrap for pmrec. NOT run by hand — provision-aws.sh renders
# the __PLACEHOLDERS__ below and passes this as the instance's user-data, so it
# runs once as root on first boot.
#
# It installs Python + git, drops a re-runnable finish-setup.sh into the app dir,
# and — if a REPO_URL was given — clones the code and starts the service. With no
# REPO_URL it preps the box and waits for you to scp the code in, then run
# finish-setup.sh yourself.
set -euxo pipefail

APP=/opt/polymarket-recorder
BUCKET="__BUCKET__"
REGION="__REGION__"
REPO_URL="__REPO_URL__"

dnf install -y python3.11 git
mkdir -p "$APP"

# finish-setup.sh: idempotent. Run as root once the code is in $APP. Builds the
# venv, writes config.yaml (bucket+region filled in), installs the systemd unit
# as ec2-user, and starts the recorder.
cat > "$APP/finish-setup.sh" <<FINISH
#!/bin/bash
set -euo pipefail
APP=$APP
cd "\$APP"
python3.11 -m venv .venv
./.venv/bin/pip install --upgrade pip
./.venv/bin/pip install -r requirements.txt
./.venv/bin/pip install -e .

if [ ! -f config.yaml ]; then
  sed -e 's|bucket: "CHANGE_ME"|bucket: "$BUCKET"|' \\
      -e 's|region: null|region: "$REGION"|' \\
      config.example.yaml > config.yaml
fi

# Run the service as ec2-user (the shipped unit assumes a 'pmrec' user).
sed 's|^User=.*|User=ec2-user|' deploy/pmrec.service \\
  > /etc/systemd/system/pmrec.service

# The service (as ec2-user) needs to own the tree so it can write ./data.
chown -R ec2-user:ec2-user "\$APP"

systemctl daemon-reload
systemctl enable --now pmrec
echo "pmrec started. Follow logs with: journalctl -u pmrec -f"
FINISH
chmod +x "$APP/finish-setup.sh"

if [ -n "$REPO_URL" ]; then
  tmp="$(mktemp -d)"
  git clone "$REPO_URL" "$tmp"
  cp -a "$tmp/." "$APP/"
  rm -rf "$tmp"
fi

if [ -f "$APP/requirements.txt" ]; then
  bash "$APP/finish-setup.sh"
else
  chown -R ec2-user:ec2-user "$APP"
  cat > "$APP/README-FIRST.txt" <<EOF
No REPO_URL was provided at launch. Copy the repo onto this box, e.g. from your
laptop:  rsync -av --exclude .venv --exclude data ./ ec2-user@<this-ip>:$APP/
then on the box run:  sudo $APP/finish-setup.sh
EOF
fi
