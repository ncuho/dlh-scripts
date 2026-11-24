#!/usr/bin/env python3
"""Translate read-only Greenplum/PostgreSQL ad-hoc SQL into Trino; execute nothing."""
import argparse
from pathlib import Path
import sys

import sqlglot
from sqlglot import exp
from sqlglot.errors import ErrorLevel

LAYERS={'oil_stg':'bronze','oil_dds':'silver','oil_mart':'gold'}


def convert(sql, lakehouse=False):
    """Translate complete SELECTs, optionally remap layers, and reject unreviewed constructs."""
    expressions=sqlglot.parse(sql,read='postgres',error_level=ErrorLevel.RAISE)
    result=[]
    for tree in expressions:
        if tree is None:
            continue
        if not isinstance(tree,exp.Query):
            raise ValueError('Only SELECT/WITH/UNION ad-hoc queries are supported; DDL/procedures are separate')
        # GP text::json parses an object. Trino CAST(varchar AS JSON) instead wraps
        # the text as a JSON string; JSON_PARSE preserves the GP text-input meaning.
        # The input of an explicit ::json/::jsonb cast must therefore be textual.
        tree=tree.transform(lambda node: exp.ParseJSON(this=node.this.copy())
                            if isinstance(node,exp.Cast) and node.to.this in
                            (exp.DataType.Type.JSON,exp.DataType.Type.JSONB) else node)
        for node in tree.walk():
            # SELECT can hide a modifying CTE or a sequence/UDF call; parsing alone is not validation.
            if isinstance(node,(exp.DDL,exp.DML,exp.Command,exp.Into)):
                raise ValueError('A modifying or unsupported statement is embedded in the query')
            if isinstance(node,exp.Anonymous):
                raise ValueError('Unmapped function requires explicit review: '+node.name)
            if isinstance(node,exp.DataType):
                if node.this==exp.DataType.Type.DECIMAL and not node.expressions:
                    raise ValueError('Unbounded PostgreSQL numeric requires explicit Trino precision and scale')
                if node.this in (exp.DataType.Type.TIMESTAMP,exp.DataType.Type.TIMESTAMPTZ) and not node.expressions:
                    # GP defaults to microseconds; Trino defaults to milliseconds unless precision is explicit.
                    node.set('expressions',[exp.DataTypeParam(this=exp.Literal.number(6))])
            if isinstance(node,exp.Table) and node.db in ('pg_catalog','information_schema','gp_toolkit'):
                raise ValueError('System catalog query has no automatic Lakehouse equivalent')
        if lakehouse:
            for table in tree.find_all(exp.Table):
                if table.db in LAYERS:
                    # Change qualified physical relations only; CTE names and literals stay untouched.
                    table.set('db',exp.to_identifier(LAYERS[table.db]))
                    table.set('catalog',exp.to_identifier('dlh'))
        rendered=tree.sql(dialect='trino',pretty=True,unsupported_level=ErrorLevel.RAISE)
        sqlglot.parse_one(rendered,read='trino',error_level=ErrorLevel.RAISE)
        result.append(rendered.rstrip(';')+';')
    if not result:
        raise ValueError('No queries found')
    return '\n\n\n'.join(result)+'\n'


def main():
    """Read a UTF-8 file/stdin and write formatted SQL only after every statement validates."""
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('input',nargs='?',type=Path,help='GP SQL file; omit to read stdin')
    parser.add_argument('-o','--output',type=Path)
    parser.add_argument('--lakehouse',action='store_true',help='Map oil_stg/dds/mart to dlh.bronze/silver/gold')
    args=parser.parse_args()
    source=args.input.read_text() if args.input else sys.stdin.read()
    target=convert(source,args.lakehouse)
    if args.output:
        args.output.write_text(target)
    else:
        sys.stdout.write(target)


if __name__=='__main__':
    try:main()
    except Exception as error:
        print('Conversion stopped: '+str(error),file=sys.stderr)
        sys.exit(1)
