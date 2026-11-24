"""Run exactly the deployed business entry point from a Mac terminal, without local secrets.

AS IS used Airflow to call GP functions. This CLI uses the same target Python
entry point as the two DLH DAGs; execution and credentials remain in the worker.
The shared filesystem writer lock also excludes concurrent Airflow runs.
"""
import argparse
import datetime
import json
import shlex
import subprocess


def main():
    """Select an explicit mode and execute once; never retry a lost external response automatically."""
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode',choices=['catchup','release'])
    parser.add_argument('--overlap-offsets',type=int,default=10)
    parser.add_argument('--check',action='store_true',help='Read-only target baseline and connection preflight')
    args=parser.parse_args()
    if not 0<=args.overlap_offsets<=10000:parser.error('overlap must be 0..10000')
    run_id='cli_'+args.mode+'_'+datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%S%f')
    program='import sys,json\nsys.path.insert(0,"/opt/airflow/dags/managed/oil_layered_migration")\nfrom oil_dlh_jobs import execute\nresult=execute('+repr(args.mode)+','+repr(run_id)+','+repr(args.overlap_offsets)+')\nprint(json.dumps(result))\n'
    if args.check:
        program='import sys,json\nsys.path.insert(0,"/opt/airflow/dags/managed/oil_layered_migration")\nfrom oil_dlh_jobs import client,Processor,writer_lock,ROOT\nwith writer_lock(ROOT):\n p=Processor(client(),None,"release","cli_preflight")\n b=p.baseline()\n print(json.dumps({"status":"PASS","accepted_run_id":b["run_id"],"partitions":b["manifest"]["partitions"]}))\n'
    command=['kubectl','exec','-i','-n','airflow-atomcluster-4jn2dw24','airflow-worker-0','-c','worker','--','python','-']
    # SSH uses the operator's configured identity; service tokens are never copied to the Mac.
    result=subprocess.run(['ssh','-o','BatchMode=yes','redos@79.137.175.114',shlex.join(command)],input=program,text=True)
    if result.returncode:raise SystemExit('Execution failed or acknowledgement was lost; inspect receipt for '+run_id+' before retry')


if __name__=='__main__':main()
