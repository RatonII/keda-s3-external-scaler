package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log"
	"net"
	"os"
	"strconv"
	"time"

	pb "github.com/external-scaler/s3-files/externalscaler"
)

const (
	targetObjectCountMetricName     = "targetObjectCount"
	activationObjectCountMetricName = "activationObjectCount"
	defaultTargetObjectCount        = 5
	//defaultBlobDelimiter          = "/"
	//defaultBlobPrefix             = ""
)

type ExternalScaler struct{}

func (e *ExternalScaler) IsActive(ctx context.Context, scaledObject *pb.ScaledObjectRef) (*pb.IsActiveResponse, error) {
	s3BucketName := scaledObject.ScalerMetadata["s3BucketName"]
	if s3BucketName == "" {
		return nil, status.Error(codes.InvalidArgument, "s3 bucket name must be specified")
	}
	objectCount := defaultTargetObjectCount
	var err error
	if scaledObject.ScalerMetadata[targetObjectCountMetricName] != "" {
		objectCount, err = strconv.Atoi(scaledObject.ScalerMetadata[targetObjectCountMetricName])
		if err != nil {
			log.Println(fmt.Errorf("%s", err))
		}
	}
	// Using the Config value, create the DynamoDB client
	s3ObjectsCount, err := getS3ObjectsCount(s3BucketName)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &pb.IsActiveResponse{
		Result: s3ObjectsCount > objectCount,
	}, nil
}

func (e *ExternalScaler) GetMetricSpec(ctx context.Context, scaledObject *pb.ScaledObjectRef) (*pb.GetMetricSpecResponse, error) {
	objectCount := int64(defaultTargetObjectCount)
	var err error
	if scaledObject.ScalerMetadata[targetObjectCountMetricName] != "" {
		objectCount, err = strconv.ParseInt(scaledObject.ScalerMetadata[targetObjectCountMetricName], 10, 64)
		if err != nil {
			log.Println(fmt.Errorf("%s", err))
		}
	}
	return &pb.GetMetricSpecResponse{
		MetricSpecs: []*pb.MetricSpec{{
			MetricName: "s3ObjectsThreshold",
			TargetSize: objectCount,
		}},
	}, nil
}

func (e *ExternalScaler) GetMetrics(_ context.Context, metricRequest *pb.GetMetricsRequest) (*pb.GetMetricsResponse, error) {
	if metricRequest.MetricName == "" || metricRequest.ScaledObjectRef == nil {
		return nil, status.Error(codes.InvalidArgument, "metricName and scaledObjectRef fields must not be empty")
	}
	if metricRequest.ScaledObjectRef.ScalerMetadata == nil {
		return nil, status.Error(codes.InvalidArgument, "scaledObjectRef.scalerMetadata field must not be empty")
	}
	s3BucketName := metricRequest.ScaledObjectRef.ScalerMetadata["s3BucketName"]
	if s3BucketName == "" {
		return nil, status.Error(codes.InvalidArgument, "scaledObjectRef.scalerMetadata.s3BucketName must not be empty")
	}
	s3ObjectsCount, err := getS3ObjectsCount(s3BucketName)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &pb.GetMetricsResponse{
		MetricValues: []*pb.MetricValue{{
			MetricName:  "s3ObjectsThreshold",
			MetricValue: int64(s3ObjectsCount),
		}},
	}, nil
}

func getS3ObjectsCount(s3BucketName string) (int, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}
	// Using the Config value, create the DynamoDB client
	s3Client := s3.NewFromConfig(cfg)
	attr, err := s3Client.ListObjects(context.Background(), &s3.ListObjectsInput{
		Bucket:              &s3BucketName,
		ExpectedBucketOwner: nil,
	})
	if err != nil {
		log.Fatalf("unable to get s3 list of objects, %v", err)
	}
	return int(len(attr.Contents)), nil
}

func (e *ExternalScaler) StreamIsActive(scaledObject *pb.ScaledObjectRef, epsServer pb.ExternalScaler_StreamIsActiveServer) error {
	s3BucketName := scaledObject.ScalerMetadata["s3BucketName"]
	objectCount := defaultTargetObjectCount
	var err error
	if scaledObject.ScalerMetadata[targetObjectCountMetricName] != "" {
		objectCount, err = strconv.Atoi(scaledObject.ScalerMetadata[targetObjectCountMetricName])
		if err != nil {
			log.Println(fmt.Errorf("%s", err))
		}
	}
	if s3BucketName == "" {
		return status.Error(codes.InvalidArgument, "s3 bucket name must be specified")
	}

	for {
		select {
		case <-epsServer.Context().Done():
			// call cancelled
			return nil
		case <-time.Tick(time.Minute * 2):
			s3ObjectsCount, err := getS3ObjectsCount(s3BucketName)
			if err != nil {
				// log error
				log.Println(fmt.Errorf("%s", err))
			} else if s3ObjectsCount > objectCount {
				err = epsServer.Send(&pb.IsActiveResponse{
					Result: true,
				})
			}
		}
	}
}

func main() {
	grpcServer := grpc.NewServer()
	grpcPort := "6000"
	if os.Getenv("PORT") != "" {
		grpcPort = os.Getenv("PORT")
	}
	lis, _ := net.Listen("tcp", fmt.Sprintf(":%s", grpcPort))
	pb.RegisterExternalScalerServer(grpcServer, &ExternalScaler{})

	fmt.Printf("listening on :%s", grpcPort)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatal(err)
	}
}
