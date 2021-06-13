from datetime import datetime, timezone


def convert_epoch_to_datetime(epoch):
    return datetime.fromtimestamp(epoch, tz=timezone.utc)
