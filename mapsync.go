package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand/v2"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	aws_ev "github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type FileAction int

const (
	DownloadAction FileAction = 0
	UpdateAction   FileAction = 1
	RemoveAction   FileAction = 2
)

const (
	downloadTimeout = 15 * time.Minute
	requestTimeout  = time.Minute
)

type LocalFileChange struct {
	Name   string
	Action FileAction
	MTime  time.Time
}

type syncJob struct {
	object   *s3.GetObjectInput
	filename string
	key      string
	mtime    time.Time
}

type syncError struct {
	key      string
	filename string
	err      error
}

type S3DownloadSync struct {
	Bucket          string
	Region          string
	Prefix          string
	Path            string
	maxKeys         int32
	concurrentFiles int
	s3Service       *s3.Client
	logger          *log.Logger
}

type SyncService struct {
	Bucket       string
	Region       string
	Prefix       string
	Path         string
	QueueUrl     string
	FIFOPath     string
	s3Sync       *S3DownloadSync
	sqsSvc       *sqs.Client
	logger       *log.Logger
	lastFullSync time.Time
	lastSync     time.Time
	lastChange   atomic.Pointer[time.Time]
	changes      []string
	changesMutex sync.Mutex
	fullSync     chan struct{}
}

type RemoteObjectInfo struct {
	Size         *int64
	LastModified *time.Time
}

type SyncPlan []*LocalFileChange

func compareFileObjects(remoteObj *RemoteObjectInfo, localObj os.FileInfo) bool {
	if remoteObj.LastModified == nil || remoteObj.Size == nil {
		// if object have no date or size assume that it was changed
		return false
	}

	timeDiff := (*remoteObj.LastModified).UTC().Sub(localObj.ModTime().UTC()).Truncate(time.Second)
	sizeDiff := *remoteObj.Size - localObj.Size()
	return (sizeDiff == 0) && math.Abs(timeDiff.Seconds()) < 1
}

func NewSyncService(endpoint, bucket, region, prefix, path, queue, fifoPath string) *SyncService {
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	logger := log.New(os.Stderr, "", 0)
	cfg, err := config.LoadDefaultConfig(
		context.Background(),
		config.WithRegion(region),
		// an empty list, unlike nil, means that no ~/.aws/config is read
		config.WithSharedConfigFiles([]string{}),
	)
	if err != nil {
		logger.Fatalf("Can't load AWS config: %v", err)
	}
	if endpoint != "" {
		// local AWS emulator (floci, ministack, etc)
		cfg.BaseEndpoint = aws.String(endpoint)
	}

	s3Svc := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = endpoint != ""
	})
	sqsSvc := sqs.NewFromConfig(cfg)

	downloadSync := &S3DownloadSync{
		Bucket:          bucket,
		Region:          region,
		Prefix:          prefix,
		Path:            path,
		maxKeys:         800,
		concurrentFiles: 5,
		s3Service:       s3Svc,
		logger:          logger,
	}

	return &SyncService{
		Bucket:   bucket,
		Region:   region,
		Prefix:   prefix,
		Path:     path,
		QueueUrl: queue,
		FIFOPath: fifoPath,
		s3Sync:   downloadSync,
		sqsSvc:   sqsSvc,
		changes:  make([]string, 0, 256),
		logger:   logger,
		fullSync: make(chan struct{}, 1),
	}
}

func (s *SyncService) Sync(ctx context.Context) error {
	changed, err := s.s3Sync.Sync(ctx)
	if err != nil {
		return err
	}

	s.lastSync = time.Now()
	s.lastFullSync = time.Now()
	if changed {
		s.AfterSyncChanges()
	}
	return nil
}

func (s *SyncService) PartialSync(ctx context.Context, keys []string) error {
	changed, err := s.s3Sync.PartialSync(ctx, keys)
	if err != nil {
		return err
	}

	s.lastSync = time.Now()
	if changed {
		s.AfterSyncChanges()
	}
	return nil
}

// Start syncs until ctx is cancelled. Then it stops everything but the
// downloads in progress, and returns once they are finished.
func (s *SyncService) Start(ctx context.Context) {
	// initial sync
	err := s.Sync(ctx)
	if err != nil && ctx.Err() == nil {
		s.logger.Fatalf("Initial sync failed: %v", err)
	}

	var group sync.WaitGroup
	group.Go(func() { s.periodicSync(ctx) })
	group.Go(func() { s.ProcessMessages(ctx) })
	group.Wait()
}

func (s *SyncService) RequestFullSync() {
	select {
	case s.fullSync <- struct{}{}:
	default:
		// a full sync is requested already
	}
}

func (s *SyncService) periodicSync(ctx context.Context) {
	changesTicker := time.NewTicker(time.Second * 30)
	defer changesTicker.Stop()
	fullSyncTicker := time.NewTicker(time.Hour * 3)
	defer fullSyncTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-changesTicker.C:
			lastChange := s.lastChange.Load()
			if lastChange == nil || time.Since(*lastChange) < time.Second*90 {
				// wait till there will 90 seconds after last change
				// this will agregate multiple changes
				// and will give some time to undo changes
				continue
			}
			s.changesMutex.Lock()

			if len(s.changes) == 0 {
				s.changesMutex.Unlock()
				// no changes
				continue
			}

			keys := make([]string, len(s.changes))
			copy(keys, s.changes)
			s.changes = s.changes[:0]
			s.changesMutex.Unlock()

			if err := s.PartialSync(ctx, keys); err != nil && ctx.Err() == nil {
				s.logger.Printf("Failed partial sync: %v", err)
			}
		case <-fullSyncTicker.C:
			// full sync
			if time.Since(s.lastFullSync) > 24*time.Hour && time.Since(s.lastSync) > 2*time.Hour {
				if err := s.Sync(ctx); err != nil && ctx.Err() == nil {
					s.logger.Printf("Failed to sync: %v", err)
				}
			}
		case <-s.fullSync:
			if err := s.Sync(ctx); err != nil && ctx.Err() == nil {
				s.logger.Printf("Failed to sync: %v", err)
			}
		}
	}
}

func (s *SyncService) AfterSyncChanges() {
	const rescan = "rescan_pending 1\n"

	files, err := os.ReadDir(s.FIFOPath)
	if err != nil {
		s.logger.Printf("Can't list FIFO directory %v", err)
		return
	}

	for _, file := range files {
		if file.Type()&os.ModeNamedPipe == 0 {
			continue
		}
		filename := filepath.Join(s.FIFOPath, file.Name())
		fifo, err := os.OpenFile(filename, os.O_WRONLY|syscall.O_NONBLOCK, os.ModeNamedPipe)
		if err != nil {
			s.logger.Printf("Can't open fifo %s: %v", filename, err)
			continue
		}
		_, err = fifo.Write([]byte(rescan))
		if err != nil {
			s.logger.Printf("Can't write into fifo: %s %v", filename, err)
		}
		fifo.Close()
	}
}

func (s *SyncService) addChange(key string) {
	now := time.Now()
	s.lastChange.Store(&now)

	s.changesMutex.Lock()
	s.changes = append(s.changes, key)
	s.changesMutex.Unlock()
}

func (s *SyncService) ProcessMessages(ctx context.Context) {
	for ctx.Err() == nil {
		input := &sqs.ReceiveMessageInput{
			MaxNumberOfMessages: 10,
			QueueUrl:            aws.String(s.QueueUrl),
			WaitTimeSeconds:     20,
		}
		receiveCtx, cancel := context.WithTimeout(ctx, requestTimeout)
		msgs, err := s.sqsSvc.ReceiveMessage(receiveCtx, input)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			s.logger.Printf("Error during reading messages: %v", err)
			select {
			case <-ctx.Done():
			case <-time.After(time.Second * 5):
			}
			continue
		}

		for _, message := range msgs.Messages {
			var snsMessage aws_ev.SNSEntity
			var s3Event aws_ev.S3Event

			deleteCtx, cancel := context.WithTimeout(ctx, requestTimeout)
			_, err := s.sqsSvc.DeleteMessage(deleteCtx, &sqs.DeleteMessageInput{
				QueueUrl:      aws.String(s.QueueUrl),
				ReceiptHandle: message.ReceiptHandle,
			})
			cancel()
			if err != nil {
				s.logger.Printf("Error during removing message: %v", err)
			}

			err = json.Unmarshal([]byte(*message.Body), &snsMessage)
			if err != nil {
				s.logger.Printf("Error during unpacking message: %v", err)
				continue
			}

			err = json.Unmarshal([]byte(snsMessage.Message), &s3Event)
			if err != nil {
				s.logger.Printf("Error during unpacking message: %v", err)
				continue
			}
			for _, item := range s3Event.Records {
				if item.EventSource != "aws:s3" {
					continue
				}

				if item.S3.Bucket.Name != s.Bucket {
					// wrong bucket, something strange
					continue
				}

				// S3 URL-encodes keys in events, the library decodes them
				objectKey := item.S3.Object.URLDecodedKey
				if strings.HasPrefix(objectKey, s.Prefix) {
					// good message
					s.logger.Printf("Received change event: %s, type: %s, ip %s",
						objectKey, item.EventName,
						item.RequestParameters.SourceIPAddress)
					key := strings.TrimPrefix(objectKey, s.Prefix)
					s.addChange(key)
				}
			}
		}
	}
}

func createSyncPlan(remoteObjects map[string]*RemoteObjectInfo,
	localObjects map[string]os.FileInfo,
) SyncPlan {
	fileChanges := make([]*LocalFileChange, 0, 256)
	sortedKeys := make([]string, 0, len(remoteObjects))

	for k := range remoteObjects {
		sortedKeys = append(sortedKeys, k)
	}

	sort.Strings(sortedKeys)

	for _, name := range sortedKeys {
		objInfo, ok := remoteObjects[name]

		if !ok {
			continue // it's imposible
		}

		localObjInfo, ok := localObjects[name]
		// TODO: Maybe remove processed local objects ?
		if !ok {
			action := &LocalFileChange{
				Name:   name,
				Action: DownloadAction,
				MTime:  *objInfo.LastModified,
			}
			fileChanges = append(fileChanges, action)
			continue
		}

		same := compareFileObjects(objInfo, localObjInfo)
		if !same {
			action := &LocalFileChange{
				Name:   name,
				Action: UpdateAction,
				MTime:  *objInfo.LastModified,
			}
			fileChanges = append(fileChanges, action)
		}
	}

	for name := range localObjects {
		_, ok := remoteObjects[name]
		if !ok {
			action := &LocalFileChange{
				Name:   name,
				Action: RemoveAction,
			}
			fileChanges = append(fileChanges, action)
		}
	}

	return fileChanges
}

// maxNameLen is the longest name whose temp file (see createTempFile) still
// fits into the 255-byte file name limit of Linux file systems.
const maxNameLen = 255 - len(".") - len(".4294967295.tmp")

func createTempFile(filename string) (*os.File, error) {
	dir, base := filepath.Split(filename)
	for {
		name := filepath.Join(dir, fmt.Sprintf(".%s.%d.tmp", base, rand.Uint32()))
		file, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o666)
		if os.IsExist(err) {
			continue
		}
		return file, err
	}
}

func writeObject(client *s3.Client, object *s3.GetObjectInput, w io.Writer) error {
	ctx, cancel := context.WithTimeout(context.Background(), downloadTimeout)
	defer cancel()
	resp, err := client.GetObject(ctx, object)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, err = io.Copy(w, resp.Body)
	return err
}

func downloadObject(client *s3.Client, task syncJob) (err error) {
	file, err := createTempFile(task.filename)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			os.Remove(file.Name())
		}
	}()

	err = writeObject(client, task.object, file)
	closeErr := file.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err = os.Chtimes(file.Name(), time.Now(), task.mtime); err != nil {
		return err
	}
	return os.Rename(file.Name(), task.filename)
}

// downloadObjectTask downloads the jobs until ctx is cancelled, then it skips
// the rest of them. A download in progress isn't cancelled with ctx.
func downloadObjectTask(ctx context.Context, client *s3.Client, jobs <-chan syncJob,
	errors chan<- syncError, group *sync.WaitGroup,
) {
	for task := range jobs {
		if ctx.Err() != nil {
			continue
		}
		if err := downloadObject(client, task); err != nil {
			errors <- syncError{
				key:      task.key,
				filename: task.filename,
				err:      err,
			}
		}
	}
	group.Done()
}

func (s *S3DownloadSync) localObjects() (map[string]os.FileInfo, error) {
	localObjects := make(map[string]os.FileInfo)

	// collect list of local objects, maps are only synced at the top level
	entries, err := os.ReadDir(s.Path)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return nil, err
		}
		localObjects[entry.Name()] = info
	}
	return localObjects, nil
}

// validName is name that does not include `/` (since subdirectories are not allowed)
// nor matches usual filenames that exist inside any folder (. or ..)
// and isn't longer than maxNameLen bytes
func (s *S3DownloadSync) validName(name string) bool {
	if name == "" || name == "." || name == ".." || strings.Contains(name, "/") {
		s.logger.Printf("Skipping key %q: it's not a file directly under the prefix", s.Prefix+name)
		return false
	}
	if len(name) > maxNameLen {
		s.logger.Printf("Skipping key %q: its name is longer than %d bytes", s.Prefix+name, maxNameLen)
		return false
	}
	return true
}

func (s *S3DownloadSync) remoteObjects(ctx context.Context) (map[string]*RemoteObjectInfo, error) {
	remoteObjects := make(map[string]*RemoteObjectInfo)

	bucketOptions := &s3.ListObjectsV2Input{
		Bucket:  aws.String(s.Bucket),
		Prefix:  aws.String(s.Prefix),
		MaxKeys: aws.Int32(s.maxKeys),
	}

	// load list of remote objects
	pages := s3.NewListObjectsV2Paginator(s.s3Service, bucketOptions)
	for pages.HasMorePages() {
		pageCtx, cancel := context.WithTimeout(ctx, requestTimeout)
		page, err := pages.NextPage(pageCtx)
		cancel()
		if err != nil {
			return nil, err
		}
		for _, object := range page.Contents {
			name := strings.TrimPrefix(*object.Key, s.Prefix)
			if !s.validName(name) {
				continue
			}
			remoteObjects[name] = &RemoteObjectInfo{
				LastModified: object.LastModified,
				Size:         object.Size,
			}
		}
	}

	return remoteObjects, nil
}

func (s *S3DownloadSync) PartialSync(ctx context.Context, keys []string) (bool, error) {
	// TODO: log more
	s.logger.Println("started partial sync")

	remoteObjects := make(map[string]*RemoteObjectInfo)
	localObjects := make(map[string]os.FileInfo)

	for _, name := range keys {
		if !s.validName(name) {
			continue
		}

		// a map that isn't there yet still has to be checked in S3
		locInfo, err := os.Stat(filepath.Join(s.Path, name))
		if err == nil {
			localObjects[name] = locInfo
		} else if !os.IsNotExist(err) {
			return false, err
		}

		objKey := s.Prefix + name
		headOptions := &s3.HeadObjectInput{
			Bucket: aws.String(s.Bucket),
			Key:    aws.String(objKey),
		}

		headCtx, cancel := context.WithTimeout(ctx, requestTimeout)
		resp, err := s.s3Service.HeadObject(headCtx, headOptions)
		cancel()
		// process errors
		if err != nil {
			var notFound *types.NotFound
			if errors.As(err, &notFound) {
				continue
			}
			return false, err
		}
		remoteObjects[name] = &RemoteObjectInfo{
			LastModified: resp.LastModified,
			Size:         resp.ContentLength,
		}
	}

	plan := createSyncPlan(remoteObjects, localObjects)

	if len(plan) == 0 {
		s.logger.Println("done, nothing changed")
		return false, nil
	} else {
		return true, s.executeSync(ctx, plan)
	}
}

func (s *S3DownloadSync) Sync(ctx context.Context) (bool, error) {
	s.logger.Println("started sync")
	localObjects, err := s.localObjects()
	if err != nil {
		return false, err
	}

	remoteObjects, err := s.remoteObjects(ctx)
	if err != nil {
		return false, err
	}

	plan := createSyncPlan(remoteObjects, localObjects)

	if len(plan) == 0 {
		s.logger.Println("done, nothing changed")
		return false, nil
	}

	return true, s.executeSync(ctx, plan)
}

func (s *S3DownloadSync) executeSync(ctx context.Context, plan SyncPlan) error {
	jobs := make(chan syncJob, s.concurrentFiles*6)
	errors := make(chan syncError, 10)
	waitGroup := &sync.WaitGroup{}
	waitGroup.Add(s.concurrentFiles)

	for i := 0; i < s.concurrentFiles; i++ {
		go downloadObjectTask(ctx, s.s3Service, jobs, errors, waitGroup)
	}

	errorsLogged := make(chan struct{})
	go func() {
		for item := range errors {
			s.logger.Printf("Error during processing %s: %v", item.key, item.err)
		}
		close(errorsLogged)
	}()

	for _, item := range plan {
		if ctx.Err() != nil {
			// shutting down, the rest is left to the next sync
			break
		}
		localFile := filepath.Join(s.Path, item.Name)

		if item.Action == RemoveAction {
			// nothing to download
			err := os.Remove(localFile)
			if err != nil {
				s.logger.Printf("Can't remove file: %s %v", item.Name, err)
			} else {
				s.logger.Printf("Remove file: %s", item.Name)
			}
			continue
		}

		obj := &s3.GetObjectInput{
			Bucket: aws.String(s.Bucket),
			Key:    aws.String(s.Prefix + item.Name),
		}

		s.logger.Printf("Sync file: %s", item.Name)

		select {
		case jobs <- syncJob{
			object:   obj,
			filename: localFile,
			key:      item.Name,
			mtime:    item.MTime.Local(),
		}:
		case <-ctx.Done():
		}
	}
	close(jobs)
	waitGroup.Wait()
	close(errors)
	<-errorsLogged
	s.logger.Println("done, sync ended")
	return nil
}

/*
Used environment variables:
	AWS_REGION - aws region
	S3_BUCKET - bucket for sync
	S3_PREFIX - bucket prefix
	SQS_QUEUE_URL - url to queue with update events
	AWS_ENDPOINT_URL - optional, endpoint of local AWS emulator for testing
	AWS_DEFAULT_REGION
	AWS_ACCESS_KEY_ID
	AWS_SECRET_ACCESS_KEY

Signals:
	SIGHUP - do a full sync
	SIGTERM, SIGINT - stop once the downloads in progress are finished, the
		second one stops immediately

Usage example:
	mapsync -path /opt/xonotic/maps/
*/

func loadParam(name string) string {
	val, ok := os.LookupEnv(name)
	if !ok {
		fmt.Fprintf(os.Stderr, "Please provide environment variable: %s", name)
		os.Exit(1)
	}
	return val
}

func main() {
	pathPtr := flag.String("path", "", "local fs path to directory for sync")
	fifoPathPtr := flag.String("fifo_path", "", "path to admin sockets")
	flag.Parse()

	bucket := loadParam("S3_BUCKET")
	region := loadParam("AWS_REGION")
	prefix := loadParam("S3_PREFIX")
	queueUrl := loadParam("SQS_QUEUE_URL")
	// endpoint is optional parameter
	endpoint := os.Getenv("AWS_ENDPOINT_URL")

	service := NewSyncService(endpoint, bucket, region, prefix, *pathPtr, queueUrl, *fifoPathPtr)

	// the first SIGTERM or SIGINT stops mapsync once the downloads in progress
	// are finished, the second one terminates it immediately
	ctx, cancel := context.WithCancel(context.Background())
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-signals
		// back to the default handling, which terminates the process
		signal.Stop(signals)
		service.logger.Printf("Shutting down (%v), waiting for the downloads in progress", sig)
		cancel()
	}()

	hangups := make(chan os.Signal, 1)
	signal.Notify(hangups, syscall.SIGHUP)
	go func() {
		for sig := range hangups {
			service.logger.Printf("Full sync requested (%v)", sig)
			service.RequestFullSync()
		}
	}()

	service.Start(ctx)
	service.logger.Println("Stopped")
}
