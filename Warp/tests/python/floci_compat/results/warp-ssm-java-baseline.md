# Floci java / ssm vs warp (baseline: before the AWS frontends) (2026-09-25 20:56)

endpoint `http://localhost:49728`, runner rc=1, 1.7s

| status | count |
|---|---|
| pass | 0 |
| fail | 1 |
| error | 15 |
| skip | 0 |

| file | pass | fail | error | skip |
|---|---|---|---|---|
| com.floci.test.SsmTest | 0 | 1 | 15 | 0 |

## Failures (class: a=not implemented, b=wrong behaviour, c=Floci-specific, d=environment; heuristic)

- [b] `com.floci.test.SsmTest::putParameter` -- Missing or unrecognized X-Amz-Target header: AmazonSSM.PutParameter (Service: Ssm, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.SsmTest::getParameter` -- Missing or unrecognized X-Amz-Target header: AmazonSSM.GetParameter (Service: Ssm, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.SsmTest::labelParameterVersion` -- Missing or unrecognized X-Amz-Target header: AmazonSSM.LabelParameterVersion (Service: Ssm, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.SsmTest::getParameterHistory` -- Missing or unrecognized X-Amz-Target header: AmazonSSM.GetParameterHistory (Service: Ssm, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.SsmTest::getParameters` -- Missing or unrecognized X-Amz-Target header: AmazonSSM.GetParameters (Service: Ssm, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.SsmTest::describeParameters` -- Missing or unrecognized X-Amz-Target header: AmazonSSM.DescribeParameters (Service: Ssm, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.SsmTest::getParametersByPath` -- Missing or unrecognized X-Amz-Target header: AmazonSSM.GetParametersByPath (Service: Ssm, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [a] `com.floci.test.SsmTest::addTagsToResource` -- Missing or unrecognized X-Amz-Target header: AmazonSSM.AddTagsToResource (Service: Ssm, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [a] `com.floci.test.SsmTest::listTagsForResource` -- Missing or unrecognized X-Amz-Target header: AmazonSSM.ListTagsForResource (Service: Ssm, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [a] `com.floci.test.SsmTest::removeTagsFromResource` -- Missing or unrecognized X-Amz-Target header: AmazonSSM.RemoveTagsFromResource (Service: Ssm, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.SsmTest::deleteParameter` -- Missing or unrecognized X-Amz-Target header: AmazonSSM.DeleteParameter (Service: Ssm, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.SsmTest::deleteParameters` -- Missing or unrecognized X-Amz-Target header: AmazonSSM.PutParameter (Service: Ssm, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.SsmTest::sendCommandRejectsTimeoutBelowAwsMinimum` -- expected: "ValidationException"
- [b] `com.floci.test.SsmTest::sendCommandCreatesPendingInvocation` -- Missing or unrecognized X-Amz-Target header: AmazonSSM.SendCommand (Service: Ssm, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.SsmTest::listRunCommandsAndInvocations` -- Missing or unrecognized X-Amz-Target header: AmazonSSM.ListCommands (Service: Ssm, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
- [b] `com.floci.test.SsmTest::cancelCommandUpdatesInvocationStatus` -- Missing or unrecognized X-Amz-Target header: AmazonSSM.CancelCommand (Service: Ssm, Status Code: 400, Request ID: null) (SDK Attempt Count: 1)
