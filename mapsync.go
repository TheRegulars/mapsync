package main

import (
	"encoding/json"
	"flag"
	"fmt"
	aws_ev "github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/sqs"
	"io/ioutil"
	"log"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

type FileAction int

const (
	DownloadAction FileAction = 0
	UpdateAction   FileAction = 1
	RemoveAction   FileAction = 2
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
	maxKeys         int64
	concurrentFiles int
	s3Service       *s3.S3
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
	session      *session.Session
	sqsSvc       *sqs.SQS
	logger       *log.Logger
	lastFullSync time.Time
	lastSync     time.Time
	lastChange   time.Time
	changes      []string
	changesMutex sync.Mutex
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

func NewSyncService(bucket, region, prefix, path, queue, fifoPath string) *SyncService {
	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(region),
	}))

	logger := log.New(os.Stderr, "", 0)
	s3Svc := s3.New(sess)
	sqsSvc := sqs.New(sess)

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
		session:  sess,
		sqsSvc:   sqsSvc,
		changes:  make([]string, 0, 256),
		logger:   logger,
	}
}

func (s *SyncService) Sync() error {
	changed, err := s.s3Sync.Sync()

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

func (s *SyncService) PartialSync(keys []string) error {
	changed, err := s.s3Sync.PartialSync(keys)

	if err != nil {
		return err
	}

	s.lastSync = time.Now()
	if changed {
		s.AfterSyncChanges()
	}
	return nil
}

func (s *SyncService) Start() {
	// initial sync
	err := s.Sync()
	if err != nil {
		s.logger.Fatalf("Initial sync failed: %v", err)
	}
	go s.periodicSync()
	s.ProcessMessages()
}

func (s *SyncService) periodicSync() {
	for {
		select {
		case <-time.After(time.Second * 30):
			if s.lastChange.IsZero() || time.Since(s.lastChange) < time.Second*90 {
				// wait till there will 90 seconds after last change
				// this will agregate multiple changes
				// and will give time to undo changes
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

			if err := s.PartialSync(keys); err != nil {
				s.logger.Printf("Failed partial sync: %v", err)
			}
		case <-time.After(time.Hour * 3):
			// full sync
			if time.Since(s.lastFullSync) > 24*time.Hour && time.Since(s.lastSync) > 2*time.Hour {
				if err := s.Sync(); err != nil {
					s.logger.Printf("Failed to sync: %v", err)
				}
			}
		}
	}
}

func (s *SyncService) AfterSyncChanges() {
	const rescan = "rescan_pending 1\n"

	files, err := ioutil.ReadDir(s.FIFOPath)
	if err != nil {
		s.logger.Printf("Can't list FIFO directory %v", err)
		return
	}

	for _, file := range files {
		if file.Mode()&os.ModeNamedPipe == 0 {
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
	s.changesMutex.Lock()
	s.changes = append(s.changes, key)
	s.lastChange = time.Now()
	s.changesMutex.Unlock()
}

func (s *SyncService) ProcessMessages() {
	for {
		input := &sqs.ReceiveMessageInput{
			MaxNumberOfMessages: aws.Int64(10),
			QueueUrl:            aws.String(s.QueueUrl),
		}
		msgs, err := s.sqsSvc.ReceiveMessage(input)

		if err != nil {
			s.logger.Printf("Error during reading messages: %v", err)
			time.Sleep(time.Second * 5)
			continue
		}

		if len(msgs.Messages) < 1 {
			time.Sleep(time.Second * 7)
			continue
		}

		for _, message := range msgs.Messages {
			var snsMessage aws_ev.SNSEntity
			var s3Event aws_ev.S3Event

			_, err := s.sqsSvc.DeleteMessage(&sqs.DeleteMessageInput{
				QueueUrl:      aws.String(s.QueueUrl),
				ReceiptHandle: message.ReceiptHandle,
			})

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

				if strings.HasPrefix(item.S3.Object.Key, s.Prefix) {
					// good message
					s.logger.Printf("Received change event: %s, type: %s, ip %s",
						item.S3.Object.Key, item.EventName,
						item.RequestParameters.SourceIPAddress)
					key := strings.TrimPrefix(item.S3.Object.Key, s.Prefix)
					s.addChange(key)
				}
			}
		}
	}
}

func createSyncPlan(remoteObjects map[string]*RemoteObjectInfo,
	localObjects map[string]os.FileInfo) SyncPlan {

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

	for name, _ := range localObjects {
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

func downloadObjectTask(downloader *s3manager.Downloader, jobs <-chan syncJob,
	errors chan<- syncError, group *sync.WaitGroup) {

	var update bool
	var err error

	for task := range jobs {
		var file *os.File

		if _, err = os.Stat(task.filename); os.IsNotExist(err) {
			update = false
			file, err = os.Create(task.filename)
			if err != nil {
				goto Error
			}
		} else {
			update = true
			file, err = ioutil.TempFile(filepath.Dir(task.filename), filepath.Base(task.filename))
			if err != nil {
				goto Error
			}
		}

		_, err = downloader.Download(file, task.object)
		file.Close()

		if err != nil {
			if update {
				// remove partially downloaded temporary file
				os.Remove(file.Name())
			}
			goto Error
		}

		if update {
			if err = os.Rename(file.Name(), task.filename); err != nil {
				goto Error
			}
		}
		err = os.Chtimes(task.filename, time.Now(), task.mtime)
		if err != nil {
			goto Error
		}

		continue
	Error:
		errors <- syncError{
			key:      task.key,
			filename: task.filename,
			err:      err,
		}
	}
	group.Done()
}

func (s *S3DownloadSync) localObjects() (map[string]os.FileInfo, error) {
	localObjects := make(map[string]os.FileInfo)

	// collect list of local objects
	err := filepath.Walk(s.Path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		name := strings.TrimPrefix(path, s.Path)
		localObjects[name] = info
		return nil
	})
	return localObjects, err
}

func (s *S3DownloadSync) remoteObjects() (map[string]*RemoteObjectInfo, error) {
	remoteObjects := make(map[string]*RemoteObjectInfo)

	bucketOptions := &s3.ListObjectsV2Input{
		Bucket:  aws.String(s.Bucket),
		Prefix:  aws.String(s.Prefix),
		MaxKeys: aws.Int64(s.maxKeys),
	}

	// load list of remote objects
	err := s.s3Service.ListObjectsV2Pages(bucketOptions,
		func(page *s3.ListObjectsV2Output, lastPage bool) bool {

			for _, object := range page.Contents {
				name := strings.TrimPrefix(*object.Key, s.Prefix)
				remoteObjects[name] = &RemoteObjectInfo{
					LastModified: object.LastModified,
					Size:         object.Size,
				}
			}
			return false
		})

	return remoteObjects, err
}

func (s *S3DownloadSync) PartialSync(keys []string) (bool, error) {
	// TODO: log more
	s.logger.Println("started partial sync")

	remoteObjects := make(map[string]*RemoteObjectInfo)
	localObjects := make(map[string]os.FileInfo)

	for _, name := range keys {
		localFile := s.Path + name

		locInfo, err := os.Stat(localFile)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return false, err
		}

		localObjects[name] = locInfo
	}

	for _, name := range keys {
		objKey := s.Prefix + name
		headOptions := &s3.HeadObjectInput{
			Bucket: aws.String(s.Bucket),
			Key:    aws.String(objKey),
		}

		resp, err := s.s3Service.HeadObject(headOptions)
		// process errors
		if err != nil {
			aerr, ok := err.(awserr.Error)
			if ok && aerr.Code() == "NotFound" {
				continue
			} else {
				return false, err
			}
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
		return true, s.executeSync(plan)
	}
}

func (s *S3DownloadSync) Sync() (bool, error) {

	s.logger.Println("started sync")
	localObjects, err := s.localObjects()

	if err != nil {
		return false, err
	}

	remoteObjects, err := s.remoteObjects()
	if err != nil {
		return false, err
	}

	plan := createSyncPlan(remoteObjects, localObjects)

	if len(plan) == 0 {
		s.logger.Println("done, nothing changed")
		return false, nil
	}

	return true, s.executeSync(plan)
}

func (s *S3DownloadSync) executeSync(plan SyncPlan) error {

	downloader := s3manager.NewDownloaderWithClient(s.s3Service)

	jobs := make(chan syncJob, s.concurrentFiles*6)
	errors := make(chan syncError, 10)
	waitGroup := &sync.WaitGroup{}

	for i := 0; i < s.concurrentFiles; i++ {
		go downloadObjectTask(downloader, jobs, errors, waitGroup)
	}
	waitGroup.Add(s.concurrentFiles)

	go func() {
		for item := range errors {
			s.logger.Printf("Error during processing %s: %v", item.key, item.err)
		}
	}()

	for _, item := range plan {
		localFile := s.Path + item.Name

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

		jobs <- syncJob{
			object:   obj,
			filename: localFile,
			key:      item.Name,
			mtime:    item.MTime.Local(),
		}
	}
	close(jobs)
	waitGroup.Wait()
	close(errors)
	s.logger.Println("done, sync ended")
	return nil
}

/*
Used environment variables:
	AWS_REGION - aws region
	S3_BUCKET - bucket for sync
	S3_PREFIX - bucket prefix
	SQS_QUEUE_URL - url to queue with update events
	AWS_DEFAULT_REGION
	AWS_ACCESS_KEY_ID
	AWS_SECRET_ACCESS_KEY

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

	service := NewSyncService(bucket, region, prefix, *pathPtr, queueUrl, *fifoPathPtr)
	service.Start()
}
