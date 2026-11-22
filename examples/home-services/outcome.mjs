// voice-ci outcome command for the sample agent.
// stdin: {"tool": "finish_call", "args": {...}}  ->  stdout: {"kind", "record"}
// kind picks the closing line; record is the part of the call that must come out the same
// on every correct run, so voice-ci can compare it with the suite's expected record.
import { readFileSync } from 'node:fs';

const { args = {} } = JSON.parse(readFileSync(0, 'utf8') || '{}');

const verdict = ['customer', 'message', 'spam'].includes(args.verdict) ? args.verdict : 'message';
const customer = verdict === 'customer';
const job = customer && args.job === true;
const urgent = customer && args.urgent === true;

// Compare only what a correct call must get right: the last name and the house number.
const lastName = (n) => String(n || '').trim().split(/\s+/).pop().toLowerCase().replace(/[^a-z]/g, '') || null;
const houseNumber = (a) => (String(a || '').match(/\d+/) || [null])[0];

const kind = verdict === 'spam' ? 'spam' : verdict === 'message' ? 'message' : urgent ? 'urgent' : 'job';

process.stdout.write(JSON.stringify({
  kind,
  record: {
    verdict,
    job,
    urgent,
    last_name: verdict === 'spam' ? null : lastName(args.name),
    house_number: job ? houseNumber(args.address) : null,
  },
}));
