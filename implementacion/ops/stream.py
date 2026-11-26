import argparse

from settings import OWNER, STREAM, session


def ensure(client):
    try:
        info = client.describe_stream_summary(StreamName=STREAM)["StreamDescriptionSummary"]
    except client.exceptions.ResourceNotFoundException:
        client.create_stream(StreamName=STREAM, ShardCount=1, StreamModeDetails={"StreamMode": "PROVISIONED"},
                             Tags={"Project": OWNER})
        client.get_waiter("stream_exists").wait(StreamName=STREAM, WaiterConfig={"Delay": 3, "MaxAttempts": 60})
        info = client.describe_stream_summary(StreamName=STREAM)["StreamDescriptionSummary"]
    consumers = list(client.get_paginator("list_stream_consumers").paginate(StreamARN=info["StreamARN"]))
    if (info["StreamStatus"] != "ACTIVE" or info["OpenShardCount"] != 1 or
            info["RetentionPeriodHours"] != 24 or info["StreamModeDetails"]["StreamMode"] != "PROVISIONED" or
            any(page["Consumers"] for page in consumers)):
        raise RuntimeError("El stream existente difiere de R11")
    return info["StreamARN"]


def remove(client):
    try:
        tags = client.list_tags_for_stream(StreamName=STREAM)["Tags"]
    except client.exceptions.ResourceNotFoundException:
        return
    if {"Key": "Project", "Value": OWNER} not in tags:
        raise RuntimeError("El stream no está etiquetado como recurso de pruebas del parcial")
    client.delete_stream(StreamName=STREAM)
    client.get_waiter("stream_not_exists").wait(StreamName=STREAM, WaiterConfig={"Delay": 3, "MaxAttempts": 60})


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("create", "delete"))
    args = parser.parse_args()
    client = session().client("kinesis")
    if args.action == "create":
        print(ensure(client))
    else:
        remove(client)
        print("Stream de pruebas retirado")
