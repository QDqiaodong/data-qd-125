DESCRIPTION='Deterministic sessionization and lookback conversion attribution'
import argparse, json, sqlite3, sys
from pathlib import Path
from .common import read, save
from .model import analyze
from . import commission, erase, pipeline, privacy

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
    erase_parser=commands.add_parser('erase',help='erase users and retain permanent tombstones')
    erase_parser.add_argument('--db',type=Path,required=True)
    erase_parser.add_argument('--request',type=Path,required=True)
    args=parser.parse_args(argv)
    if args.command=='apply':
        summary=pipeline.apply_batch(args.db,args.source)
    elif args.command=='erase':
        summary=erase.erase(args.db,args.request)
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

COMMISSION_COMMANDS=('commission-init','commission-sync','payout-plan','payout-confirm','payout-cancel','commission-report')

def commission_main(argv):
    parser=argparse.ArgumentParser(description=DESCRIPTION+' (offline commission ledger and payout reservations)')
    commands=parser.add_subparsers(dest='command',required=True)
    init_parser=commands.add_parser('commission-init',help='create a ledger with fixed channel basis-point rates')
    init_parser.add_argument('--ledger',type=Path,required=True)
    init_parser.add_argument('--rates',type=Path,required=True)
    sync_parser=commands.add_parser('commission-sync',help='apply one committed source snapshot to the ledger')
    sync_parser.add_argument('--source',type=Path,required=True)
    sync_parser.add_argument('--ledger',type=Path,required=True)
    sync_parser.add_argument('--sync-id',type=str,required=True)
    plan_parser=commands.add_parser('payout-plan',help='atomically reserve commission for a multi-channel plan')
    plan_parser.add_argument('--ledger',type=Path,required=True)
    plan_parser.add_argument('--request',type=Path,required=True)
    confirm_parser=commands.add_parser('payout-confirm',help='convert an active reservation into a permanent paid record')
    confirm_parser.add_argument('--ledger',type=Path,required=True)
    confirm_parser.add_argument('--plan-id',type=str,required=True)
    cancel_parser=commands.add_parser('payout-cancel',help='release an active reservation')
    cancel_parser.add_argument('--ledger',type=Path,required=True)
    cancel_parser.add_argument('--plan-id',type=str,required=True)
    report_parser=commands.add_parser('commission-report',help='per-channel accrued/paid/reserved/debt report')
    report_parser.add_argument('--ledger',type=Path,required=True)
    report_parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args(argv)
    if args.command=='commission-init':
        summary=commission.init_ledger(args.ledger,args.rates)
    elif args.command=='commission-sync':
        summary=commission.sync(args.source,args.ledger,args.sync_id)
    elif args.command=='payout-plan':
        summary=commission.plan_payout(args.ledger,args.request)
    elif args.command=='payout-confirm':
        summary=commission.confirm_payout(args.ledger,args.plan_id)
    elif args.command=='payout-cancel':
        summary=commission.cancel_payout(args.ledger,args.plan_id)
    else:
        summary=commission.report(args.ledger,args.output)
    print(json.dumps(summary,sort_keys=True,ensure_ascii=False,allow_nan=False));return 0

def main(argv=None):
    argv=list(sys.argv[1:] if argv is None else argv)
    try:
        if argv and argv[0] in ('apply','export','erase'):
            return incremental_main(argv)
        if argv and argv[0] in ('privacy-init','privacy-release'):
            return privacy_main(argv)
        if argv and argv[0] in COMMISSION_COMMANDS:
            return commission_main(argv)
        return legacy_main(argv)
    except (ValueError,TypeError,KeyError,OSError,OverflowError,RecursionError,
            sqlite3.IntegrityError,sqlite3.Error) as error:
        print(str(error),file=sys.stderr);return 2

if __name__=='__main__':raise SystemExit(main())
