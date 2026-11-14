import unittest,tempfile,subprocess,sys,json,hashlib
from pathlib import Path
from visit_attribution.cli import run
ROOT=Path(__file__).resolve().parents[1]
class CLITests(unittest.TestCase):
    def test_installed_cli_report_replay_and_source_hash(self):
        with tempfile.TemporaryDirectory() as td:
            source=ROOT/'fixtures/input.json';out=Path(td)/'report.json'
            command=[sys.executable,'-m','visit_attribution.cli','--source',str(source),'--output',str(out)]
            r=subprocess.run(command,capture_output=True);self.assertEqual(r.returncode,0,r.stderr)
            report=json.loads(out.read_text());self.assertEqual(report,run(source))
            self.assertEqual(report['source_sha256'],hashlib.sha256(source.read_bytes()).hexdigest())
            before=out.read_bytes();self.assertEqual(subprocess.run(command,capture_output=True).returncode,0);self.assertEqual(out.read_bytes(),before)
    def test_bad_input_preserves_report_and_source(self):
        with tempfile.TemporaryDirectory() as td:
            p=Path(td);source=p/'bad.json';out=p/'out.json';out.write_text('old')
            for raw in ['{}','{"a":1,"a":2}','NaN']:
                source.write_text(raw)
                r=subprocess.run([sys.executable,'-m','visit_attribution.cli','--source',str(source),'--output',str(out)],capture_output=True)
                self.assertEqual(r.returncode,2);self.assertEqual(out.read_text(),'old')
            before=source.read_bytes();r=subprocess.run([sys.executable,'-m','visit_attribution.cli','--source',str(source),'--output',str(source)],capture_output=True)
            self.assertEqual(r.returncode,2);self.assertEqual(source.read_bytes(),before)
if __name__=='__main__':unittest.main()
