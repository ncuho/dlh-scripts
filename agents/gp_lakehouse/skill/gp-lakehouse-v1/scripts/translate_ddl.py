#!/usr/bin/env python3
"""Deterministically translate explicit GP table schemas into Iceberg v2 DDL for Trino."""
import argparse
from pathlib import Path
import re
import sys

# The uploaded skill bundles the pinned pure-Python SQLGlot package for its offline sandbox.
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'lib'))
import sqlglot
from sqlglot import exp
from sqlglot.errors import ErrorLevel

LAYERS={'oil_stg':'bronze','oil_dds':'silver','oil_mart':'gold'}


def translate(source):
    """Drop physical GP distribution/defaults/constraints explicitly; report DQ obligations in comments."""
    # Strip only the known trailing GP clause. Anything else must parse strictly, not become Command.
    cleaned=re.sub(r'\bDISTRIBUTED\s+(?:BY\s*\([^;]*?\)|RANDOMLY|REPLICATED)\s*(?=;|$)','',source,flags=re.I)
    statements=sqlglot.parse(cleaned,read='postgres',error_level=ErrorLevel.RAISE);result=[]
    for tree in statements:
        if tree is None:continue
        if not isinstance(tree,exp.Create) or tree.kind!='TABLE' or not isinstance(tree.this,exp.Schema) or tree.expression:
            raise ValueError('Only CREATE TABLE with explicit columns is supported; views/CTAS/functions require a reviewed contract')
        if tree.args.get('properties'):
            raise ValueError('GP storage/partition properties require explicit physical-layout review')
        origin=tree.this.this
        if origin.db not in LAYERS or origin.catalog:
            raise ValueError('Choose an explicit target layer for this source namespace')
        columns=[];notes=[]
        for item in tree.this.expressions:
            if not isinstance(item,exp.ColumnDef):
                notes.append(item.sql(dialect='postgres'));continue
            kind=item.kind.copy()
            if kind.this in (exp.DataType.Type.TIMESTAMP,exp.DataType.Type.TIMESTAMPTZ):
                kind.set('expressions',[exp.DataTypeParam(this=exp.Literal.number(6))])
            typename=kind.sql(dialect='trino',unsupported_level=ErrorLevel.RAISE).lower()
            if not re.fullmatch(r'(?:varchar|integer|bigint|smallint|boolean|date|double|real|decimal\(\d+,\s*\d+\)|timestamp\(6\)(?: with time zone)?)',typename):
                raise ValueError('Unreviewed type mapping: '+typename)
            columns.append('    '+item.this.sql(dialect='trino')+' '+typename)
            if item.constraints:
                notes.append(item.name+': '+', '.join(c.sql(dialect='postgres') for c in item.constraints))
        if not columns:raise ValueError('No explicit columns')
        name='dlh.'+LAYERS[origin.db]+'.'+origin.this.sql(dialect='trino')
        header='-- Iceberg v2: GP distribution is not copied; keys/nullability/default behavior require ingestion/DQ rules.\n'
        if notes:header+='-- Source rules: '+' | '.join(notes).replace('\n',' ')+'\n'
        result.append(header+'CREATE TABLE '+name+' (\n'+',\n'.join(columns)+"\n) WITH (format='PARQUET', format_version=2);")
    if not result:raise ValueError('No table definitions')
    return '\n\n\n'.join(result)+'\n'


def main():
    """Create a local SQL artifact; never execute the generated DDL."""
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('input',type=Path);parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args();args.output.write_text(translate(args.input.read_text()))


if __name__=='__main__':main()
