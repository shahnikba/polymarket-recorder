#!/usr/bin/env bash
# One-shot AWS provisioner for pmrec. Run LOCALLY with admin AWS credentials.
# Creates (idempotently): the S3 bucket + 90-day Glacier lifecycle, a
# least-privilege IAM role/instance-profile (PutObject on your prefix only), and
# an EC2 instance that bootstraps itself from deploy/user-data.sh.
#
# Usage:
#   BUCKET=my-polymarket-raw KEY_NAME=my-ec2-keypair ./deploy/provision-aws.sh
#
# Required env:
#   BUCKET     globally-unique S3 bucket name
#   KEY_NAME   name of an existing EC2 key pair (for SSH)
# Optional env:
#   REGION         (default us-east-1 — best latency to Polymarket)
#   INSTANCE_TYPE  (default t3.small)
#   DISK_GB        (default 20)
#   REPO_URL       git URL of this repo; if set the box self-installs and starts.
#                  If unset, you scp the code up and run finish-setup.sh yourself.
#   SSH_CIDR       e.g. 203.0.113.4/32 — opens port 22 to that CIDR via a new SG.
#                  If unset, no inbound rule is added (recorder needs none).
set -euo pipefail

: "${BUCKET:?set BUCKET to a globally-unique bucket name}"
: "${KEY_NAME:?set KEY_NAME to your existing EC2 key pair}"
REGION="${REGION:-us-east-1}"
INSTANCE_TYPE="${INSTANCE_TYPE:-t3.small}"
DISK_GB="${DISK_GB:-20}"
REPO_URL="${REPO_URL:-}"
SSH_CIDR="${SSH_CIDR:-}"
PREFIX="polymarket/raw"
ROLE="pmrec-recorder"
HERE="$(cd "$(dirname "$0")" && pwd)"

echo ">> region=$REGION bucket=$BUCKET role=$ROLE type=$INSTANCE_TYPE"

# --- 1. S3 bucket ---------------------------------------------------------
if aws s3api head-bucket --bucket "$BUCKET" 2>/dev/null; then
  echo ">> bucket exists, skipping create"
else
  if [ "$REGION" = "us-east-1" ]; then
    aws s3api create-bucket --bucket "$BUCKET" --region "$REGION"
  else
    aws s3api create-bucket --bucket "$BUCKET" --region "$REGION" \
      --create-bucket-configuration LocationConstraint="$REGION"
  fi
fi
aws s3api put-public-access-block --bucket "$BUCKET" \
  --public-access-block-configuration \
  BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
aws s3api put-bucket-encryption --bucket "$BUCKET" \
  --server-side-encryption-configuration \
  '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'
aws s3api put-bucket-lifecycle-configuration --bucket "$BUCKET" \
  --lifecycle-configuration "{\"Rules\":[{\"ID\":\"archive-raw-to-glacier\",\"Filter\":{\"Prefix\":\"$PREFIX/\"},\"Status\":\"Enabled\",\"Transitions\":[{\"Days\":90,\"StorageClass\":\"GLACIER\"}]}]}"
echo ">> bucket configured (private, encrypted, 90d->Glacier)"

# --- 2. IAM role + instance profile (least privilege) ---------------------
if ! aws iam get-role --role-name "$ROLE" >/dev/null 2>&1; then
  aws iam create-role --role-name "$ROLE" --assume-role-policy-document \
    '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
fi
aws iam put-role-policy --role-name "$ROLE" --policy-name pmrec-s3-put \
  --policy-document "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Sid\":\"PutRawFrames\",\"Effect\":\"Allow\",\"Action\":\"s3:PutObject\",\"Resource\":\"arn:aws:s3:::$BUCKET/$PREFIX/*\"}]}"
if ! aws iam get-instance-profile --instance-profile-name "$ROLE" >/dev/null 2>&1; then
  aws iam create-instance-profile --instance-profile-name "$ROLE"
  aws iam add-role-to-instance-profile --instance-profile-name "$ROLE" --role-name "$ROLE"
fi
echo ">> IAM role + instance profile ready (PutObject on $PREFIX/* only)"

# --- 3. Optional SSH security group --------------------------------------
SG_ARGS=()
if [ -n "$SSH_CIDR" ]; then
  VPC=$(aws ec2 describe-vpcs --region "$REGION" \
    --filters Name=isDefault,Values=true --query 'Vpcs[0].VpcId' --output text)
  SG=$(aws ec2 create-security-group --region "$REGION" \
    --group-name "pmrec-ssh-$(date +%s 2>/dev/null || echo x)" \
    --description "pmrec SSH" --vpc-id "$VPC" --query GroupId --output text)
  aws ec2 authorize-security-group-ingress --region "$REGION" --group-id "$SG" \
    --protocol tcp --port 22 --cidr "$SSH_CIDR"
  SG_ARGS=(--security-group-ids "$SG")
  echo ">> SSH security group $SG (port 22 from $SSH_CIDR)"
fi

# --- 4. Render user-data and launch --------------------------------------
AMI=$(aws ssm get-parameter --region "$REGION" \
  --name /aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64 \
  --query Parameter.Value --output text)

UD="$(mktemp)"
sed -e "s|__BUCKET__|$BUCKET|g" -e "s|__REGION__|$REGION|g" \
    -e "s|__REPO_URL__|$REPO_URL|g" "$HERE/user-data.sh" > "$UD"

# Retry: a freshly-created instance profile can take a few seconds to propagate.
IID=""
for attempt in 1 2 3 4 5 6; do
  if IID=$(aws ec2 run-instances --region "$REGION" \
      --image-id "$AMI" --instance-type "$INSTANCE_TYPE" \
      --iam-instance-profile "Name=$ROLE" \
      --key-name "$KEY_NAME" \
      --user-data "file://$UD" \
      --block-device-mappings "[{\"DeviceName\":\"/dev/xvda\",\"Ebs\":{\"VolumeSize\":$DISK_GB,\"VolumeType\":\"gp3\"}}]" \
      --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=pmrec}]' \
      "${SG_ARGS[@]}" \
      --query 'Instances[0].InstanceId' --output text 2>/tmp/pmrec-run.err); then
    break
  fi
  echo ">> launch attempt $attempt failed (likely IAM propagation), retrying..."
  sleep 10
done
[ -n "$IID" ] || { echo "ERROR: launch failed:"; cat /tmp/pmrec-run.err; exit 1; }
rm -f "$UD"

echo ">> launched instance $IID; waiting for it to start..."
aws ec2 wait instance-running --region "$REGION" --instance-ids "$IID"
IP=$(aws ec2 describe-instances --region "$REGION" --instance-ids "$IID" \
  --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)

cat <<DONE

==========================================================================
 pmrec instance is up: $IID  ($IP)
 - Bootstrap runs from user-data; first install takes ~1-2 min.
 - Logs once running:   ssh ec2-user@$IP 'journalctl -u pmrec -f'
 - Verify uploads:      aws s3 ls s3://$BUCKET/$PREFIX/ --recursive | head
$( [ -z "$REPO_URL" ] && echo " - No REPO_URL set: rsync the code up, then: ssh ec2-user@$IP 'sudo /opt/polymarket-recorder/finish-setup.sh'" )
==========================================================================
DONE
