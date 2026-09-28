// Kinesis over cleartext HTTP/2 (prior knowledge) with the AWS SDK for JavaScript v3: large uploads, large concurrent responses on one
// session, several requests per connection. Run by test_awsextras_conformance.py with NODE_PATH pointing at a node_modules holding
// @aws-sdk/client-kinesis (Floci's compatibility-tests/sdk-test-node has one). Prints "OK <ms>" on success.
const { KinesisClient, CreateStreamCommand, PutRecordCommand, GetShardIteratorCommand, GetRecordsCommand, DescribeStreamCommand } = require('@aws-sdk/client-kinesis');

async function main() {
  const c = new KinesisClient({ endpoint: process.argv[2], region: 'us-east-1', credentials: { accessKeyId: 'a', secretAccessKey: 'b' }, maxAttempts: 1 });
  const name = 'h2c-' + Date.now();
  await c.send(new CreateStreamCommand({ StreamName: name, ShardCount: 1 }));
  const shard = (await c.send(new DescribeStreamCommand({ StreamName: name }))).StreamDescription.Shards[0].ShardId;
  const big = Buffer.alloc(900 * 1024, 'x');
  for (let i = 0; i < 3; i++) await c.send(new PutRecordCommand({ StreamName: name, Data: big, PartitionKey: 'pk' }));
  for (let i = 0; i < 50; i++) await c.send(new PutRecordCommand({ StreamName: name, Data: Buffer.from('small-' + i), PartitionKey: 'pk' }));
  const it = (await c.send(new GetShardIteratorCommand({ StreamName: name, ShardId: shard, ShardIteratorType: 'TRIM_HORIZON' }))).ShardIterator;
  const t0 = Date.now();
  const rs = await Promise.all(Array.from({ length: 40 }, () => c.send(new GetRecordsCommand({ ShardIterator: it, Limit: 3 }))));
  for (const r of rs) {
    if (r.Records.length !== 3 || r.Records[0].Data.length !== big.length) throw new Error('unexpected response');
  }
  console.log('OK ' + (Date.now() - t0));
  process.exit(0);
}
main().catch((e) => { console.error('FAIL', e); process.exit(1); });
setTimeout(() => { console.error('FAIL timeout'); process.exit(2); }, 60000);
