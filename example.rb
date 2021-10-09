require 'bundler/setup'
require 'unity-dynamodbstreams-event-parser'

event = {
  'eventID' => '1',
  'eventName' => 'INSERT',
  'eventVersion' => '1.0',
  'eventSource' => 'aws:dynamodb',
  'awsRegion' => 'us-east-1',
  'dynamodb' => {
    'Keys' => { 'id' => { 'N' => '101' } },
    'NewImage' => {
      'id' => { 'N' => '101' },
      'message' => { 'S' => 'Hello world!' }
    },
    'SequenceNumber' => '111',
    'SizeBytes' => 26,
    'StreamViewType' => 'NEW_AND_OLD_IMAGES',
    'ApproximateCreationDateTime' => 1633768788
  },
  'eventSourceARN' => 'arn:aws:dynamodb:us-east-1:111122223333:table/TestTable/stream/2015–05–11T21:21:33.291'
}

event_parser = Unity::DynamoDBStreams::EventParser.new
parsed_event = event_parser.call(event)
pp parsed_event
