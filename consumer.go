package consumer

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/awslabs/kinesis-aggregation/go/deaggregator"
)

var (
	ScanWithGetRecords       ScanMethod = "GetRecords"       // scan using GetRecords
	ScanWithSubscribeToShard ScanMethod = "SubscribeToShard" // scan using SubscribeToShard
)

// ScanMethod is scanning type.
type ScanMethod string

// Record wraps the record returned from the Kinesis library and
// extends to include the shard id.
type Record struct {
	*kinesis.Record
	ShardID            string
	MillisBehindLatest *int64
}

// New creates a kinesis consumer with default settings. Use Option to override
// any of the optional attributes.
func New(streamName string, opts ...Option) (*Consumer, error) {
	if streamName == "" {
		return nil, errors.New("must provide stream name")
	}

	// new consumer with noop storage, counter, and logger
	c := &Consumer{
		streamName:               streamName,
		initialShardIteratorType: kinesis.ShardIteratorTypeLatest,
		store:                    &noopStore{},
		counter:                  &noopCounter{},
		logger: &noopLogger{
			logger: log.New(ioutil.Discard, "", log.LstdFlags),
		},
		scanInterval: 250 * time.Millisecond,
		maxRecords:   10000,
		scanMethod:   ScanWithGetRecords,

		refreshSubscribeInterval: 4 * time.Minute,
		retrySubscribeInterval:   500 * time.Millisecond,
		retryRegisterCount:       20,
	}

	// override defaults
	for _, opt := range opts {
		opt(c)
	}

	// default client
	if c.client == nil {
		newSession, err := session.NewSession(aws.NewConfig())
		if err != nil {
			return nil, err
		}
		c.client = kinesis.New(newSession)
	}

	// default group consumes all shards
	if c.group == nil {
		c.group = NewAllGroup(c.client, c.store, streamName, c.logger)
	}

	if c.scanMethod == ScanWithSubscribeToShard && c.consumerName == "" && c.consumerARN == "" {
		return nil, errors.New("must consumer name or consumer ARN for subscribe")
	}

	return c, nil
}

// Consumer wraps the interaction with the Kinesis stream
type Consumer struct {
	streamName               string
	initialShardIteratorType string
	initialTimestamp         *time.Time
	client                   kinesisiface.KinesisAPI
	counter                  Counter
	group                    Group
	logger                   Logger
	store                    Store
	scanInterval             time.Duration
	maxRecords               int64
	isAggregated             bool
	shardClosedHandler       ShardClosedHandler

	scanMethod               ScanMethod
	consumerName             string
	consumerARN              string
	refreshSubscribeInterval time.Duration
	retrySubscribeInterval   time.Duration
	retryRegisterCount       int

	streamARN string
}

// ScanFunc is the type of the function called for each message read
// from the stream. The record argument contains the original record
// returned from the AWS Kinesis library.
// If an error is returned, scanning stops. The sole exception is when the
// function returns the special value ErrSkipCheckpoint.
type ScanFunc func(*Record) error

// ErrSkipCheckpoint is used as a return value from ScanFunc to indicate that
// the current checkpoint should be skipped skipped. It is not returned
// as an error by any function.
var ErrSkipCheckpoint = errors.New("skip checkpoint")

// Initialize initializes the consumer.
func (c *Consumer) Initialize(ctx context.Context) error {
	switch c.scanMethod {
	case ScanWithSubscribeToShard:
		if c.consumerARN != "" {
			break
		}
		c.logger.Log(LogConsumer, "register consumer:", LogString("consumerName", c.consumerName))
		var err error
		consumerARN, streamARN, err := c.registerConsumer(ctx, c.streamName, c.consumerName)
		if err != nil {
			return err
		}
		err = c.waitForConsumerActive(ctx, c.consumerName, consumerARN, streamARN)
		if err != nil {
			c.deregisterConsumer(c.consumerName, consumerARN, streamARN)
			return err
		}
		c.consumerARN = consumerARN
		c.streamARN = streamARN
	case ScanWithGetRecords:
		// do nothing
	}
	return nil
}

// Finalize performs the termination process of consumer.
// If you call Initialize, be sure to call Finalize.
func (c *Consumer) Finalize(ctx context.Context) error {
	switch c.scanMethod {
	case ScanWithSubscribeToShard:
		if c.consumerName == "" || c.consumerARN == "" || c.streamARN == "" {
			break
		}
		err := c.deregisterConsumer(c.consumerName, c.consumerARN, c.streamARN)
		if err != nil {
			return err
		}
	case ScanWithGetRecords:
		// do nothing
	}
	return nil
}

// Scan launches a goroutine to process each of the shards in the stream. The ScanFunc
// is passed through to each of the goroutines and called with each message pulled from
// the stream.
func (c *Consumer) Scan(ctx context.Context, fn ScanFunc) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		errc   = make(chan error, 1)
		shardc = make(chan *kinesis.Shard, 1)
	)

	go func() {
		c.group.Start(ctx, shardc)
		<-ctx.Done()
		close(shardc)
	}()

	wg := new(sync.WaitGroup)
	// process each of the shards
	for shard := range shardc {
		wg.Add(1)
		go func(shardID string) {
			defer wg.Done()
			var err error
			if c.scanMethod == ScanWithSubscribeToShard {
				err = c.SubscribeShard(ctx, c.consumerARN, shardID, fn)
			} else {
				err = c.ScanShard(ctx, shardID, fn)
			}
			if err != nil {
				select {
				case errc <- fmt.Errorf("shard %s error: %w", shardID, err):
					// first error to occur
					cancel()
				default:
					// error has already occurred
				}
			}
		}(aws.StringValue(shard.ShardId))
	}

	go func() {
		wg.Wait()
		close(errc)
	}()

	return <-errc
}

// ScanShard loops over records on a specific shard, calls the callback func
// for each record and checkpoints the progress of scan.
func (c *Consumer) ScanShard(ctx context.Context, shardID string, fn ScanFunc) error {
	// get last seq number from checkpoint
	lastSeqNum, err := c.group.GetCheckpoint(c.streamName, shardID)
	if err != nil {
		return fmt.Errorf("get checkpoint error: %w", err)
	}

	// get shard iterator
	shardIterator, err := c.getShardIterator(ctx, c.streamName, shardID, lastSeqNum)
	if err != nil {
		return fmt.Errorf("get shard iterator error: %w", err)
	}

	c.logger.Log(LogConsumer, "start scan:", LogString("shardID", shardID),
		LogString("lastSeqNum", lastSeqNum))
	defer func() {
		c.logger.Log(LogConsumer, "stop scan:", LogString("shardID", shardID))
	}()
	scanTicker := time.NewTicker(c.scanInterval)
	defer scanTicker.Stop()

	for {
		resp, err := c.client.GetRecords(&kinesis.GetRecordsInput{
			Limit:         aws.Int64(c.maxRecords),
			ShardIterator: shardIterator,
		})

		// attempt to recover from GetRecords error when expired iterator
		if err != nil {
			c.logger.Log(LogWarn, LogConsumer, "get records error:", Error(err))

			if awserr, ok := err.(awserr.Error); ok {
				if _, ok := retriableErrors[awserr.Code()]; !ok {
					return fmt.Errorf("get records error: %v", awserr.Message())
				}
			}

			shardIterator, err = c.getShardIterator(ctx, c.streamName, shardID, lastSeqNum)
			if err != nil {
				return fmt.Errorf("get shard iterator error: %w", err)
			}
		} else {
			c.logger.Log(LogDebug, LogConsumer, "get records success:",
				LogString("shardID", shardID), LogString("lastSeqNum", lastSeqNum))
			// loop over records, call callback func
			var records []*kinesis.Record
			var err error
			if c.isAggregated {
				records, err = deaggregator.DeaggregateRecords(resp.Records)
				if err != nil {
					return err
				}
			} else {
				records = resp.Records
			}
			for _, r := range records {
				select {
				case <-ctx.Done():
					return nil
				default:
					err := fn(&Record{r, shardID, resp.MillisBehindLatest})
					if err != nil && err != ErrSkipCheckpoint {
						return err
					}

					if err != ErrSkipCheckpoint {
						if err := c.group.SetCheckpoint(c.streamName, shardID, *r.SequenceNumber); err != nil {
							return err
						}
					}

					c.counter.Add("records", 1)
					lastSeqNum = *r.SequenceNumber
				}
			}

			if isShardClosed(resp.NextShardIterator, shardIterator) {
				c.logger.Log(LogConsumer, "shard closed:", LogString("shardID", shardID))
				if c.shardClosedHandler != nil {
					err := c.shardClosedHandler(c.streamName, shardID)
					if err != nil {
						return fmt.Errorf("shard closed handler error: %w", err)
					}
				}
				return nil
			}

			shardIterator = resp.NextShardIterator
		}

		// Wait for next scan
		select {
		case <-ctx.Done():
			return nil
		case <-scanTicker.C:
			continue
		}
	}
}

// SubscribeShard subscribe records on a specific shard, calls the callback func
// for each record and checkpoints the progress of scan.
func (c *Consumer) SubscribeShard(ctx context.Context, consumerARN, shardID string, fn ScanFunc) error {
	// get last seq number from checkpoint
	lastSeqNum, err := c.group.GetCheckpoint(c.streamName, shardID)
	if err != nil {
		return fmt.Errorf("get checkpoint error: %w", err)
	}

	c.logger.Log(LogConsumer, "start subscribe:",
		LogString("shardID", shardID), LogString("lastSeqNum", lastSeqNum))
	defer c.logger.Log(LogConsumer, "stop subscribe:", LogString("shardID", shardID))
	refreshTicker := time.NewTicker(c.refreshSubscribeInterval)
	defer refreshTicker.Stop()

	evStream, err := c.subscribeToShard(ctx, consumerARN, shardID, lastSeqNum)
	if err != nil {
		return fmt.Errorf("subscribeToShard error: %w", err)
	}
	defer func() {
		if evStream != nil {
			_ = evStream.Close()
		}
	}()

	var event kinesis.SubscribeToShardEventStreamEvent
	var exist bool
	for {
		hasNeedRefresh := false
		select {
		case <-ctx.Done():
			return nil
		case <-refreshTicker.C:
			hasNeedRefresh = true
		case event, exist = <-evStream.Events():
		}

		if !exist || hasNeedRefresh {
			oldEvSt := evStream
			nextStream, err := c.subscribeToShard(ctx, consumerARN, shardID, lastSeqNum)
			if err != nil {
				return fmt.Errorf("re-subscribeToShard error: %w", err)
			}
			evStream = nextStream
			_ = oldEvSt.Close()
			continue
		}

		subEvent, ok := event.(*kinesis.SubscribeToShardEvent)
		if !ok {
			return fmt.Errorf("unexpected event type: %v", event)
		}

		records := subEvent.Records
		if c.isAggregated {
			records, err = deaggregator.DeaggregateRecords(records)
			if err != nil {
				return err
			}
		}

		for _, r := range records {
			select {
			case <-ctx.Done(): // check canceled
				return nil
			default:
				err := fn(&Record{r, shardID, subEvent.MillisBehindLatest})
				switch {
				case err == ErrSkipCheckpoint:
					// skip
				case err != nil:
					return err
				default:
					if err := c.group.SetCheckpoint(c.streamName, shardID, *r.SequenceNumber); err != nil {
						return err
					}
				}

				c.counter.Add("records", 1)
				lastSeqNum = *r.SequenceNumber
			}
		}

		if isShardClosed(subEvent.ContinuationSequenceNumber, nil) {
			c.logger.Log(LogConsumer, "shard closed:", LogString("shardID", shardID))
			if c.shardClosedHandler != nil {
				err := c.shardClosedHandler(c.streamName, shardID)
				if err != nil {
					return fmt.Errorf("shard closed handler error: %w", err)
				}
			}
			return nil
		}
	}
}

var retriableErrors = map[string]struct{}{
	kinesis.ErrCodeExpiredIteratorException:               {},
	kinesis.ErrCodeProvisionedThroughputExceededException: {},
	kinesis.ErrCodeInternalFailureException:               {},
}

var retryableConsumerErrors = map[string]struct{}{
	kinesis.ErrCodeResourceInUseException: {},
}

func isShardClosed(nextShardIterator, currentShardIterator *string) bool {
	return nextShardIterator == nil || currentShardIterator == nextShardIterator
}

func (c *Consumer) getShardIterator(ctx context.Context, streamName, shardID, seqNum string) (*string, error) {
	params := &kinesis.GetShardIteratorInput{
		ShardId:    aws.String(shardID),
		StreamName: aws.String(streamName),
	}

	if seqNum != "" {
		params.ShardIteratorType = aws.String(kinesis.ShardIteratorTypeAfterSequenceNumber)
		params.StartingSequenceNumber = aws.String(seqNum)
	} else if c.initialTimestamp != nil {
		params.ShardIteratorType = aws.String(kinesis.ShardIteratorTypeAtTimestamp)
		params.Timestamp = c.initialTimestamp
	} else {
		params.ShardIteratorType = aws.String(c.initialShardIteratorType)
	}

	res, err := c.client.GetShardIteratorWithContext(aws.Context(ctx), params)
	return res.ShardIterator, err
}

func (c *Consumer) registerConsumer(ctx context.Context, streamName, consumerName string) (string, string, error) {
	var streamResp *kinesis.DescribeStreamSummaryOutput
	var err error
	for i := 0; i < c.retryRegisterCount; i++ {
		streamResp, err = c.client.DescribeStreamSummaryWithContext(
			ctx, &kinesis.DescribeStreamSummaryInput{
				StreamName: aws.String(streamName),
			})
		if err != nil {
			if err == credentials.ErrNoValidProvidersFoundInChain {
				c.logger.Log(LogWarn, LogConsumer, "describe stream summary error retry:", Error(err))
				time.Sleep(c.retrySubscribeInterval)
				continue
			}
			c.logger.Log(LogWarn, LogConsumer, "describe stream summary error:", Error(err))
			break
		}
		break // success
	}
	if err != nil {
		c.logger.Log(LogError, LogConsumer, "failed to DescribeStreamSummary:", Error(err))
		return "", "", err
	}
	streamARN := aws.StringValue(streamResp.StreamDescriptionSummary.StreamARN)

	consumerResp, err := c.client.RegisterStreamConsumerWithContext(
		ctx, &kinesis.RegisterStreamConsumerInput{
			ConsumerName: aws.String(consumerName),
			StreamARN:    streamResp.StreamDescriptionSummary.StreamARN,
		})
	if err != nil {
		c.logger.Log(LogError, LogConsumer, "register consumer error:", Error(err))
		return "", "", err
	}
	consumerARN := aws.StringValue(consumerResp.Consumer.ConsumerARN)
	c.logger.Log(LogConsumer, "register consumer success:",
		LogString("consumerARN", consumerARN),
		LogString("streamARN", streamARN))
	return consumerARN, streamARN, nil
}

func (c *Consumer) deregisterConsumer(consumerName, consumerARN, streamARN string) error {
	c.logger.Log(LogConsumer, "deregister consumer:", LogString("consumerName", consumerName),
		LogString("consumerARN", consumerARN), LogString("streamARN", streamARN))
	_, err := c.client.DeregisterStreamConsumer(&kinesis.DeregisterStreamConsumerInput{
		ConsumerARN:  aws.String(consumerARN),
		ConsumerName: aws.String(consumerName),
		StreamARN:    aws.String(streamARN),
	})
	if err != nil {
		c.logger.Log(LogError, LogConsumer, "deregister consumer error:", Error(err),
			LogString("consumerName", consumerName),
			LogString("consumerARN", consumerARN),
			LogString("streamARN", streamARN))
	}
	return err
}

func (c *Consumer) waitForConsumerActive(ctx context.Context, consumerName, consumerARN, streamARN string) error {
	params := &kinesis.DescribeStreamConsumerInput{
		ConsumerName: aws.String(consumerName),
		ConsumerARN:  aws.String(consumerARN),
		StreamARN:    aws.String(streamARN),
	}

	var res *kinesis.DescribeStreamConsumerOutput
	var err error
	c.logger.Log(LogConsumer, "describe stream consumer call start:",
		LogString("consumerName", consumerName),
		LogString("consumerARN", consumerARN),
		LogString("streamARN", streamARN))
	for i := 0; i < c.retryRegisterCount; i++ {
		res, err = c.client.DescribeStreamConsumerWithContext(ctx, params)

		if err != nil {
			c.logger.Log(LogError, LogConsumer, "describe stream consumer error:", Error(err))
			break
		}

		status := aws.StringValue(res.ConsumerDescription.ConsumerStatus)
		if status == kinesis.ConsumerStatusActive {
			break
		}
		c.logger.Log(LogWarn, LogConsumer, "describe stream consumer not active. retry:",
			LogString("ConsumerStatus", status))
		time.Sleep(c.retrySubscribeInterval)
	}
	if err != nil {
		c.logger.Log(LogError, LogConsumer, "failed to DescribeStreamConsumer:", Error(err),
			LogString("consumerName", consumerName),
			LogString("consumerARN", consumerARN),
			LogString("streamARN", streamARN))
		return err
	}

	c.logger.Log(LogConsumer, "describe stream consumer success:",
		LogString("consumerName", consumerName),
		LogString("consumerARN", consumerARN),
		LogString("streamARN", streamARN))
	return nil
}

func (c *Consumer) subscribeToShard(ctx context.Context, consumerARN, shardID, seqNum string) (*kinesis.SubscribeToShardEventStream, error) {
	params := &kinesis.SubscribeToShardInput{
		ConsumerARN:      aws.String(consumerARN),
		ShardId:          aws.String(shardID),
		StartingPosition: &kinesis.StartingPosition{},
	}

	if seqNum != "" {
		params.StartingPosition.Type = aws.String(kinesis.ShardIteratorTypeAfterSequenceNumber)
		params.StartingPosition.SequenceNumber = aws.String(seqNum)
	} else if c.initialTimestamp != nil {
		params.StartingPosition.Type = aws.String(kinesis.ShardIteratorTypeAtTimestamp)
		params.StartingPosition.Timestamp = c.initialTimestamp
	} else {
		params.StartingPosition.Type = aws.String(c.initialShardIteratorType)
	}

	var res *kinesis.SubscribeToShardOutput
	var err error
	c.logger.Log(LogConsumer, "subscribe to shard call start:",
		LogString("consumerARN", consumerARN),
		LogString("shardID", shardID),
		LogString("seqNum", seqNum))
	for i := 0; i < c.retryRegisterCount; i++ {
		res, err = c.client.SubscribeToShardWithContext(ctx, params)

		if err != nil {
			if c.isRetryableConsumerErrors(err) {
				c.logger.Log(LogWarn, LogConsumer, "subscribe to shard error retry:", Error(err))
				time.Sleep(c.retrySubscribeInterval)
				continue
			}
			c.logger.Log(LogError, LogConsumer, "subscribe to shard error:", Error(err))
			break
		}
		break // success
	}
	if err != nil {
		c.logger.Log(LogError, LogConsumer, "failed to SubscribeToShard:", Error(err),
			LogString("consumerARN", consumerARN),
			LogString("shardID", shardID),
			LogString("seqNum", seqNum))
		return nil, err
	}

	c.logger.Log(LogConsumer, "subscribe to shard success:",
		LogString("consumerARN", consumerARN),
		LogString("shardID", shardID),
		LogString("seqNum", seqNum))
	return res.EventStream, nil
}

func (c *Consumer) isRetryableConsumerErrors(err error) bool {
	if awserr, ok := err.(awserr.Error); ok {
		if _, ok := retryableConsumerErrors[awserr.Code()]; ok {
			return true
		}
	}
	return false
}
