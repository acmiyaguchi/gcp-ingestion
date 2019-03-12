#!/usr/bin/env python3

import boto3
import base64
import logging
import json
import os
import tarfile
from functools import partial

INGESTION_BEAM_ROOT = os.path.realpath(
    os.path.join(os.path.dirname(os.path.realpath(__file__)), "..")
)


def construct_schema_identifier(parsed_path):
    # ex: [test, test, test.1.schema.json]
    namespace = parsed_path[0]
    document_type = parsed_path[1]
    document_version = parsed_path[2].split(".")[1]
    return ".".join([namespace, document_type, document_version])


def construct_schema_set(path):
    """return a set containing "{namespace}.{doctype}.{doctype}" strings"""
    with tarfile.open(path, "r") as tf:
        listing = tf.getnames()
    paths = {
        construct_schema_identifier(x[2:])
        for x in map(lambda x: x.split("/"), listing)
        # mozilla-pipeline-schemas/schemas/namespace/doctype/doctype.version.schema.json
        if len(x) == 5 and x[1] == "schemas" and x[-1].endswith(".schema.json")
    }
    return paths


def get_schema_name(key):
    # Example:
    # sanitized-landfill-sample/v3/submission_date_s3=20190308/namespace=webpagetest/doc_type=webpagetest-run/doc_version=1/part-00122-tid-2954272513278013416-c06a39af-9979-41a5-8459-76412a4554b3-650.c000.json
    params = dict([x.split("=") for x in key.split("/") if "=" in x])
    return ".".join(map(params.get, ["namespace", "doc_type", "doc_version"]))


def generate_document(namespace, doctype, docver, payload):
    document = {
        "attributeMap": {
            "document_namespace": namespace,
            "document_type": doctype,
            "document_version": docver,
        },
        # payload is already a string
        "payload": base64.b64encode(payload.encode("utf-8")).decode("utf-8"),
    }
    return json.dumps(document)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    output_file = "avro-landfill-integration.ndjson"

    bucket = "telemetry-parquet"
    prefix = "sanitized-landfill-sample/v3/submission_date_s3=20190310"
    s3 = boto3.client("s3")

    objs = s3.list_objects(Bucket=bucket, Prefix=prefix)
    keys = [obj["Key"] for obj in objs["Contents"] if obj["Key"].endswith(".json")]

    schemas = construct_schema_set(os.path.join(INGESTION_BEAM_ROOT, "schemas.tar.gz"))

    fp = open(output_file, "w")
    for key in keys:
        schema_name = get_schema_name(key)
        if not schema_name in schemas:
            logging.info("schema does not exist for {}".format(schema_name))
            continue

        logging.info("Creating messages for {}".format(schema_name))
        data = (
            s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8").strip()
        )
        lines = data.split("\n")
        invalid = 0

        namespace, doctype, docver = schema_name.split(".")
        for line in lines:
            # each of the lines contains metadata with a content field
            content = json.loads(line).get("content")
            if not content:
                invalid += 1
                continue
            pubsub_message = generate_document(namespace, doctype, docver, content)
            fp.write(pubsub_message + "\n")
        logging.info("Wrote {} documents".format(len(lines) - invalid, invalid))
    fp.close()
    logging.info("Done!")
