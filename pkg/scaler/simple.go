/*
Copyright 2023 The Alibaba Cloud Serverless Authors.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package scaler

import (
	"container/list"
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/AliyunContainerService/scaler/pkg/config"
	"github.com/AliyunContainerService/scaler/pkg/model"
	"github.com/AliyunContainerService/scaler/pkg/platform_client"
	pb "github.com/AliyunContainerService/scaler/proto"
	"github.com/google/uuid"
)

type Simple struct {
	config         *config.Config
	metaData       *model.Meta
	platformClient platform_client.Client
	mu             sync.Mutex
	wg             sync.WaitGroup
	instances      map[string]*model.Instance
	idleInstance   *list.List
	// 统计

	// resourceUsage

	invocationExecutionTimeInGBs int64 // 请求执行总消耗
	totalSlotTimeInGBs           int64 // Slot 总消耗

	// coldStartTimeScore

	invocationExecutionTimeInSecs int64 // 请求执行总时间
	invocationScheduleTimeInSecs  int64 // 调度前消耗
	invocationIdleTimeInSecs      int64 // 调度后消耗

	/**
	resourceUsageScore = ["invocationExecutionTimeInGBs"] / ["totalSlotTimeInGBs"]
	coldStartTimeScore = ["invocationExecutionTimeInSecs"] /
	(["invocationExecutionTimeInSecs"] + ["invocationScheduleTimeInSecs"] + ["invocationIdleTimeInSecs"])
	**/

	resourceUsageScore int64   // 资源利用得分
	coldStartTimeScore int64   // 冷启动得分
	lastScore          float64 // 上一次的分数

	// gc
	idleDurationBeforeGC float64 // 动态存活时间
}

func New(metaData *model.Meta, config *config.Config) Scaler {
	client, err := platform_client.New(config.ClientAddr)
	if err != nil {
		log.Fatalf("client init with error: %s", err.Error())
	}
	scheduler := &Simple{
		config:               config,
		metaData:             metaData,
		platformClient:       client,
		mu:                   sync.Mutex{},
		wg:                   sync.WaitGroup{},
		instances:            make(map[string]*model.Instance),
		idleInstance:         list.New(),
		idleDurationBeforeGC: config.IdleDurationBeforeGC.Seconds(),
	}
	log.Printf("New scaler for app: %s is created", metaData.Key)
	scheduler.wg.Add(1)
	go func() {
		defer scheduler.wg.Done()
		scheduler.gcLoop()
		log.Printf("gc loop for app: %s is stoped", metaData.Key)
	}()

	return scheduler
}

func (s *Simple) Assign(ctx context.Context, request *pb.AssignRequest) (*pb.AssignReply, error) {
	start := time.Now()
	instanceId := uuid.New().String()
	// defer func() {
	// 	log.Printf("Assign, request id: %s, instance id: %s, cost %dms", request.RequestId, instanceId, time.Since(start).Milliseconds())
	// }()
	// log.Printf("Assign, request id: %s", request.RequestId)
	s.mu.Lock()
	if element := s.idleInstance.Front(); element != nil {
		instance := element.Value.(*model.Instance)
		instance.Busy = true
		instance.LastAssignTime = time.Now()
		s.idleInstance.Remove(element)
		s.mu.Unlock()
		// log.Printf("Assign, request id: %s, instance %s reused", request.RequestId, instance.Id)
		instanceId = instance.Id
		return &pb.AssignReply{
			Status: pb.Status_Ok,
			Assigment: &pb.Assignment{
				RequestId:  request.RequestId,
				MetaKey:    instance.Meta.Key,
				InstanceId: instance.Id,
			},
			ErrorMessage: nil,
		}, nil
	}
	s.mu.Unlock()

	//Create new Instance
	resourceConfig := model.SlotResourceConfig{
		ResourceConfig: pb.ResourceConfig{
			MemoryInMegabytes: request.MetaData.MemoryInMb,
		},
	}
	scheduleReqTime := time.Now()
	slot, err := s.platformClient.CreateSlot(ctx, request.RequestId, &resourceConfig)
	if err != nil {
		errorMessage := fmt.Sprintf("create slot failed with: %s", err.Error())
		log.Printf(errorMessage)
		return nil, status.Errorf(codes.Internal, errorMessage)
	}

	meta := &model.Meta{
		Meta: pb.Meta{
			Key:           request.MetaData.Key,
			Runtime:       request.MetaData.Runtime,
			TimeoutInSecs: request.MetaData.TimeoutInSecs,
		},
	}
	initTime := time.Now()
	instance, err := s.platformClient.Init(ctx, request.RequestId, instanceId, slot, meta)
	if err != nil {
		errorMessage := fmt.Sprintf("create instance failed with: %s", err.Error())
		log.Printf(errorMessage)
		return nil, status.Errorf(codes.Internal, errorMessage)
	}

	//add new instance
	s.mu.Lock()
	instance.Busy = true
	assignTime := time.Now()
	instance.ScheduleReqTime = scheduleReqTime
	instance.LastAssignTime = assignTime
	instance.FirstAssignTime = assignTime
	s.instances[instance.Id] = instance
	// 计算
	s.totalSlotTimeInGBs += assignTime.UnixMilli() - initTime.UnixMilli()
	s.invocationScheduleTimeInSecs += assignTime.UnixMilli() - start.UnixMilli()
	instance.CalTime = assignTime
	s.mu.Unlock()
	// log.Printf("request id: %s, instance %s for app %s is created, init latency: %dms", request.RequestId, instance.Id, instance.Meta.Key, instance.InitDurationInMs)

	return &pb.AssignReply{
		Status: pb.Status_Ok,
		Assigment: &pb.Assignment{
			RequestId:  request.RequestId,
			MetaKey:    instance.Meta.Key,
			InstanceId: instance.Id,
		},
		ErrorMessage: nil,
	}, nil
}

func (s *Simple) Idle(ctx context.Context, request *pb.IdleRequest) (*pb.IdleReply, error) {
	if request.Assigment == nil {
		return nil, status.Errorf(codes.InvalidArgument, fmt.Sprintf("assignment is nil"))
	}
	reply := &pb.IdleReply{
		Status:       pb.Status_Ok,
		ErrorMessage: nil,
	}
	start := time.Now()
	instanceId := request.Assigment.InstanceId
	// defer func() {
	// 	log.Printf("Idle, request id: %s, instance: %s, cost %dus", request.Assigment.RequestId, instanceId, time.Since(start).Microseconds())
	// }()
	//log.Printf("Idle, request id: %s", request.Assigment.RequestId)
	needDestroy := false
	slotId := ""
	if request.Result != nil && request.Result.NeedDestroy != nil && *request.Result.NeedDestroy {
		needDestroy = true
	}
	defer func() {
		if needDestroy {
			s.deleteSlot(ctx, request.Assigment.RequestId, slotId, instanceId, request.Assigment.MetaKey, "bad instance")
		}
	}()
	// log.Printf("Idle, request id: %s", request.Assigment.RequestId)
	s.mu.Lock()
	idleTime := time.Now()
	defer s.mu.Unlock()
	if instance := s.instances[instanceId]; instance != nil {
		slotId = instance.Slot.Id
		instance.LastIdleTime = idleTime
		if needDestroy {
			// log.Printf("request id %s, instance %s need be destroy", request.Assigment.RequestId, instanceId)
			return reply, nil
		}

		if instance.Busy == false {
			// log.Printf("request id %s, instance %s already freed", request.Assigment.RequestId, instanceId)
			return reply, nil
		}
		instance.Busy = false
		s.idleInstance.PushFront(instance)
		// 计算执行时间
		gap := start.UnixMilli() - instance.CalTime.UnixMilli()
		s.totalSlotTimeInGBs += gap
		s.invocationExecutionTimeInGBs += gap
		s.invocationExecutionTimeInSecs += gap
		instance.CalTime = start
	} else {
		return nil, status.Errorf(codes.NotFound, fmt.Sprintf("request id %s, instance %s not found", request.Assigment.RequestId, instanceId))
	}
	return &pb.IdleReply{
		Status:       pb.Status_Ok,
		ErrorMessage: nil,
	}, nil
}

func (s *Simple) deleteSlot(ctx context.Context, requestId, slotId, instanceId, metaKey, reason string) {
	// log.Printf("start delete Instance %s (Slot: %s) of app: %s", instanceId, slotId, metaKey)
	if err := s.platformClient.DestroySLot(ctx, requestId, slotId, reason); err != nil {
		log.Printf("delete Instance %s (Slot: %s) of app: %s failed with: %s", instanceId, slotId, metaKey, err.Error())
	}
}

func (s *Simple) gcLoop() {
	ticker := time.NewTicker(s.config.GcInterval)
	for range ticker.C {
		s.CalScore()
		for {
			s.mu.Lock()
			if element := s.idleInstance.Back(); element != nil {
				instance := element.Value.(*model.Instance)
				idleDuration := time.Now().Unix() - instance.LastIdleTime.Unix()
				if float64(idleDuration) > s.idleDurationBeforeGC {
					//need GC
					s.idleInstance.Remove(element)
					delete(s.instances, instance.Id)
					s.mu.Unlock()
					go func() {
						reason := fmt.Sprintf("Idle duration: %fs, excceed configured duration: %.0fs", idleDuration, s.idleDurationBeforeGC)
						ctx := context.Background()
						ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
						defer cancel()
						s.deleteSlot(ctx, uuid.NewString(), instance.Slot.Id, instance.Id, instance.Meta.Key, reason)
					}()

					continue
				}
			}
			s.mu.Unlock()
			break
		}
	}
}

func (s *Simple) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Stats{
		TotalInstance:     len(s.instances),
		TotalIdleInstance: s.idleInstance.Len(),
	}
}

func (s *Simple) CalScore() {
	// 计算
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.instances) == 0 {
		return
	}
	now := time.Now()
	var slotTimeInSecs int64 = 0 // slot 存活占用
	var execTimeInSecs int64 = 0 // slot 执行占用
	for _, instance := range s.instances {
		gap := now.UnixMilli() - instance.CalTime.UnixMilli()
		slotTimeInSecs += gap // 存活时长
		if instance.Busy {
			execTimeInSecs += gap // 执行时长
		}
		instance.CalTime = now
	}
	s.totalSlotTimeInGBs += slotTimeInSecs
	s.invocationExecutionTimeInGBs += execTimeInSecs
	s.invocationExecutionTimeInSecs += execTimeInSecs
	log.Printf("app %s stat, %d, %d, %d, %d", s.metaData.Key, s.totalSlotTimeInGBs, s.invocationExecutionTimeInGBs, s.invocationExecutionTimeInSecs, s.invocationScheduleTimeInSecs)

	// 算分
	resourceUsageScore := float64(s.invocationExecutionTimeInGBs) / float64(s.totalSlotTimeInGBs) * 50
	coldStartTimeScore := float64(s.invocationExecutionTimeInSecs) / float64((s.invocationExecutionTimeInSecs + s.invocationScheduleTimeInSecs + s.invocationIdleTimeInSecs)) * 50
	score := resourceUsageScore + coldStartTimeScore

	log.Printf("app %s score, %.4f, %.4f, %.4f", s.metaData.Key, score, resourceUsageScore, coldStartTimeScore)

	// 调整策略
	bt := resourceUsageScore / coldStartTimeScore
	if bt > 1.2 {
		s.idleDurationBeforeGC *= bt
	} else if bt < 0.8 {
		s.idleDurationBeforeGC *= bt
	}

	s.lastScore = score
}
