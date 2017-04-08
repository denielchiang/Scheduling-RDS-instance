import boto3
from botocore.client import ClientError
from datetime import datetime, timedelta, tzinfo

rds = boto3.client('rds')

# db_source_instance is a varible that your insance name
db_source_instance='dev'

# dev_snapshot_prefix is the snapshot name after execute this function
dev_snapshot_prefix = "dev-to-staging-snapshot"

# You can skip this class if you don't need convert to your timezone
class TWST(tzinfo):
    def utcoffset(self, dt):
        return timedelta(hours=8)
    def dst(self, dt):
        return timedelta(0)
    def tzname(self, dt):
        return 'JST'

# Lambda function entry
def lambda_handler(event, context):
    # Before we take snapshot for DEV, I prefer to delete the snapshots that I took in the same day.
    delete_snapshots(dev_snapshot_prefix)
    create_dev_snapshot(dev_snapshot_prefix, db_source_instance)

# Delete snapshots
def delete_snapshots(prefix):
    print '[delete_snapshots] Start'
    snapshots = rds.describe_db_snapshots()

    for snapshot in snapshots['DBSnapshots']:
        if snapshot['DBSnapshotIdentifier'].startswith(prefix):
            rds.delete_db_snapshot(DBSnapshotIdentifier=snapshot['DBSnapshotIdentifier'])

    print '[delete_snapshots] End'

# Create snapshot for DEV
def create_dev_snapshot(prefix, instanceid):
    print '[create_dev_snapshot] Start'
    snapshotid = "-".join([prefix, datetime.now(tz=TWST()).strftime("%Y-%m-%d")])

    for i in range(0, 5):
        try:
            snapshot = rds.create_db_snapshot(
                DBSnapshotIdentifier=snapshotid,
                DBInstanceIdentifier=instanceid
            )
            return
        except ClientError as e:
                print(str(e))
        time.sleep(1)

    print '[create_dev_snapshot] End'
