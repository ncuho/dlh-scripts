"""Publish a new DAG source under the configured managed S3 prefix."""
from __future__ import annotations
import ast
import hashlib
import json
import os
import re
from pathlib import Path
from botocore.config import Config
from botocore.exceptions import ClientError
import botocore.session


def register(mcp, settings, *, include_mutating, allowlist):
    if not include_mutating or (allowlist is not None and 'publish_dag' not in allowlist):
        return

    @mcp.tool()
    async def publish_dag(filename: str, source: str) -> str:
        """Publish a new Python DAG file to the managed DAG prefix, without overwriting existing files.

        The scheduler runs submitted Python. Publish only reviewed, authorised code.
        Publication does not itself confirm that Airflow parsed or executed the DAG.
        """
        if not re.fullmatch(r'[A-Za-z][A-Za-z0-9_]{0,95}\.py', filename):
            raise ValueError('filename must be a simple Python filename')
        content = source.encode('utf-8')
        if not content or len(content) > 262144:
            raise ValueError('source must contain 1 to 262144 bytes')
        try:
            ast.parse(source, filename=filename)
        except SyntaxError as exc:
            raise ValueError('DAG has invalid Python syntax at line %s' % exc.lineno) from None
        def storage(name: str) -> str:
            root = os.environ.get('DAG_STORAGE_SECRET_DIR')
            return (Path(root) / name).read_text().strip() if root else os.environ[name]
        prefix=storage('aws_s3_folder').strip('/') + '/managed/'
        digest=hashlib.sha256(content).hexdigest()
        key=prefix+filename[:-3]+'__'+digest+'.py'
        session=botocore.session.get_session()
        root=os.environ.get('DAG_STORAGE_SECRET_DIR')
        session_token=(Path(root) / 'AWS_SESSION_TOKEN').read_text().strip() if root and (Path(root) / 'AWS_SESSION_TOKEN').exists() else None
        client=session.create_client('s3',endpoint_url=storage('aws_s3_endpoint'),region_name=storage('aws_s3_region'),aws_access_key_id=storage('aws_s3_key'),aws_secret_access_key=storage('aws_s3_key_secret'),aws_session_token=session_token,config=Config(s3={'addressing_style':'path'},connect_timeout=10,read_timeout=30,retries={'max_attempts':1},request_checksum_calculation='when_required',response_checksum_validation='when_required'))
        bucket=storage('aws_s3_bucket')
        try:
            existing=client.get_object(Bucket=bucket,Key=key)['Body'].read(262145)
        except ClientError as exc:
            code=exc.response.get('Error',{}).get('Code','unknown')
            if code not in ('NoSuchKey','NotFound','404'):
                raise RuntimeError('S3 lookup failed (code %s)' % code) from None
            existing=None
        if existing is not None:
            if hashlib.sha256(existing).hexdigest() != digest:
                raise RuntimeError('Existing content-addressed object has unexpected content')
            return json.dumps({'status':'already_published','key':key,'sha256':digest,'bytes':len(content),'next':'verify DAG parse status and import errors'})
        try:
            client.put_object(Bucket=bucket,Key=key,Body=content,ContentType='text/x-python')
        except ClientError as exc:
            code=exc.response.get('Error',{}).get('Code','unknown')
            if code in ('PreconditionFailed','412','ConditionalRequestConflict','409'):
                raise ValueError('DAG file already exists; choose a new versioned filename') from None
            raise RuntimeError('S3 publication failed (code %s)' % code) from None
        return json.dumps({'status':'published','key':key,'sha256':digest,'bytes':len(content),'next':'verify DAG parse status and import errors'})

    # This optional tool is a strict subset of publish_dag: no arbitrary Python crosses the model boundary.
    reviewed_dir = os.environ.get('REVIEWED_DAG_DIR')
    if reviewed_dir:
        @mcp.tool()
        async def publish_reviewed_dag(artifact_id: str = 'oil-production-1.1') -> str:
            """Publish the hash-pinned oil_production_spark_candidate without sending source code.

            Only artifact_id=oil-production-1.1 is accepted. Never triggers or unpauses a DAG.
            """
            if artifact_id != 'oil-production-1.1':
                raise ValueError('Unknown reviewed artifact; do not publish unreviewed code')
            path = Path(reviewed_dir) / 'oil_production_spark_candidate.py'
            content = path.read_bytes()
            expected = '1cde59335dc80a7c4883b8e1c50dc415d470876cea29ffa70ef3acf0fe688f35'
            if hashlib.sha256(content).hexdigest() != expected:
                raise ValueError('Reviewed candidate fingerprint differs; publication blocked')
            return await publish_dag('oil_production_spark_candidate.py', content.decode('utf-8'))
