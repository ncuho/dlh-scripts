#!/usr/bin/env python3
"""Render hash-verified migration artifacts; never connect to a database or execute a DAG."""
import argparse
import ast
import hashlib
import json
from pathlib import Path
import shutil
import zipfile

ROOT=Path(__file__).resolve().parents[1]


def digest(data):
    """Bind artifact and input identity to exact bytes, without timestamps or random IDs."""
    return hashlib.sha256(data).hexdigest()


def build(output,mode):
    """Copy reviewed code only after every source and target asset matches the packaged contract."""
    profile=json.loads((ROOT/'assets/profile.json').read_text())
    for name,expected in profile['files'].items():
        actual=(ROOT/'assets'/name).read_bytes()
        if digest(actual)!=expected:
            raise ValueError('Profile code changed: '+name+'; rebuild/review the profile before translation')
    output=Path(output);output.mkdir(parents=True,exist_ok=True)
    selected=[]
    for entry in profile['artifacts']:
        if mode!='all' and entry['mode'] not in (mode,'all'):continue
        target=output/entry['path'];target.parent.mkdir(parents=True,exist_ok=True)
        content=(ROOT/'assets'/entry['asset']).read_bytes()
        if target.suffix=='.py':ast.parse(content,filename=entry['path'])
        target.write_bytes(content);selected.append(entry['path'])
    hashes={name:digest((output/name).read_bytes()) for name in sorted(selected)}
    result={'status':'PREPARED','profile':profile['version'],'mode':mode,'ddl_objects':20 if mode in ('all','ddl') else 0,
            'dag_id':'oil_production_spark_candidate' if mode in ('all','logic') else None,
            'source_sha256':profile['source_sha256'],'files':hashes,'no_external_operations':True}
    (output/'result.json').write_text(json.dumps(result,ensure_ascii=False,sort_keys=True,indent=2)+'\n')
    # Fixed ZIP headers make the downloadable bundle repeatable as well as its contents.
    # STORED avoids zlib-version-dependent bytes between local macOS and AI Space Linux.
    with zipfile.ZipFile(output/'migration-code.zip','w',compression=zipfile.ZIP_STORED) as archive:
        for name in sorted(selected+['result.json']):
            info=zipfile.ZipInfo(name,date_time=(2026,1,1,0,0,0));info.compress_type=zipfile.ZIP_STORED;info.create_system=3
            info.external_attr=0o100644<<16
            archive.writestr(info,(output/name).read_bytes())
    result['zip_sha256']=digest((output/'migration-code.zip').read_bytes())
    summary={key:result[key] for key in ['status','profile','mode','ddl_objects','dag_id','zip_sha256','no_external_operations']}
    summary['file_count']=len(hashes)
    print(json.dumps(summary,ensure_ascii=False))
    return result


def main():
    """Select DDL, business logic or all artifacts using one bounded command."""
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--mode',choices=['ddl','logic','all'],default='all')
    args=parser.parse_args();build(args.output,args.mode)


if __name__=='__main__':main()
