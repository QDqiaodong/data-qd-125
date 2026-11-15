DESCRIPTION='Deterministic sessionization and lookback conversion attribution'
import argparse, json, sqlite3, sys
from pathlib import Path
from .common import read, save
from .model import analyze
from . import pipeline, privacy

def run(source):
    data,digest=read(source)
    return {'schema_version':1,'source_sha256':digest,'result':analyze(data)}

def legacy_main(argv):
    parser=argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument('--source',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args(argv)
    if args.source.resolve()==args.output.resolve():raise ValueError('input and output must differ')
    report=run(args.source);save(args.output,report)
    print(json.dumps(report,sort_keys=True,ensure_ascii=False,allow_nan=False));return 0

def incremental_main(argv):
    parser=argparse.ArgumentParser(description=DESCRIPTION+' (SQLite incremental pipeline)')
    commands=parser.add_subparsers(dest='command',required=True)
    apply_parser=commands.add_parser('apply',help='ingest one batch file into the SQLite store')
    apply_parser.add_argument('--db',type=Path,required=True)
    apply_parser.add_argument('--source',type=Path,required=True)
    export_parser=commands.add_parser('export',help='stream the accumulated result to JSON')
    export_parser.add_argument('--db',type=Path,required=True)
    export_parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args(argv)
    if args.command=='apply':
        summary=pipeline.apply_batch(args.db,args.source)
    else:
        if args.db.resolve()==args.output.resolve():raise ValueError('database and output must differ')
        summary=pipeline.export_db(args.db,args.output)
        summary={'output':str(args.output),**summary}
    print(json.dumps(summary,sort_keys=True,ensure_ascii=False,allow_nan=False));return 0

def privacy_main(argv):
    parser=argparse.ArgumentParser(description=DESCRIPTION+' (differentially private releases)')
    commands=parser.add_subparsers(dest='command',required=True)
    init_parser=commands.add_parser('privacy-init',help='create a ledger with a fixed epsilon budget')
    init_parser.add_argument('--ledger',type=Path,required=True)
    init_parser.add_argument('--budget',type=str,required=True)
    release_parser=commands.add_parser('privacy-release',help='publish noisy day/channel distinct-user counts')
    release_parser.add_argument('--source',type=Path,required=True)
    release_parser.add_argument('--ledger',type=Path,required=True)
    release_parser.add_argument('--request',type=Path,required=True)
    release_parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args(argv)
    if args.command=='privacy-init':
        summary=privacy.init_ledger(args.ledger,args.budget)
    else:
        summary=privacy.release(args.source,args.ledger,args.request,args.output)
        summary={'output':str(args.output),**summary}
    print(json.dumps(summary,sort_keys=True,ensure_ascii=False,allow_nan=False));return 0

def main(argv=None):
    argv=list(sys.argv[1:] if argv is None else argv)
    try:
        if argv and argv[0] in ('apply','export'):
            return incremental_main(argv)
        if argv and argv[0] in ('privacy-init','privacy-release'):
            return privacy_main(argv)
        return legacy_main(argv)
    except (ValueError,TypeError,KeyError,OSError,OverflowError,RecursionError,
            sqlite3.IntegrityError,sqlite3.Error) as error:
        print(str(error),file=sys.stderr);return 2

if __name__=='__main__':raise SystemExit(main())
