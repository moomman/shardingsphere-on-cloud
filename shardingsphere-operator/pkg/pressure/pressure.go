/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package pressure

import (
	"context"
	"database/sql"
	"time"

	"github.com/apache/shardingsphere-on-cloud/shardingsphere-operator/api/v1alpha1"
	_ "github.com/go-sql-driver/mysql"
)

type Pressure struct {
	Active bool
	Name   string
	Result Result
	Err    error
}

var (
	db       *sql.DB
	totalReq int
)

type Result struct {
	//total exec req Number
	Total int
	//total success req Number
	Success int
	//todo: get total or get every exec

	//total time in this Pressure execution
	Duration time.Duration
}

// todo: get conn args by labels over string
func initDB(connArgs string) error {
	var err error
	db, err = sql.Open("mysql", connArgs)
	if err != nil {
		return err
	}
	if err := db.Ping(); err != nil {
		return err
	}
	return nil
}

func (a *Pressure) Run(ctx context.Context, chaos *v1alpha1.ShardingSphereChaos) {
	a.Active = true
	totalReq = 0
	if err := initDB(chaos.Spec.PressureCfg.SsHost); err != nil {
		a.Err = err
		return
	}
	result := &a.Result
	pressureCtx, cancel := context.WithTimeout(context.Background(), chaos.Spec.PressureCfg.Duration.Duration)
	defer cancel()
	ticker := time.NewTicker(chaos.Spec.PressureCfg.ReqTime.Duration)
	res := make(chan bool, 1000)

	//handle result
	go handleResponse(ctx, res, result)

FOR:
	for {
		select {
		case <-ctx.Done():
			break FOR
		case <-pressureCtx.Done():
			break FOR
		case <-ticker.C:
			for i := 0; i < chaos.Spec.PressureCfg.ConcurrentNum; i++ {
				totalReq += chaos.Spec.PressureCfg.ReqNum
				//todo: handle err
				go exec(pressureCtx, chaos.Spec.PressureCfg.DistSqls, chaos.Spec.PressureCfg.ReqNum, res)
			}
		}
	}

	//occur when channel closed
	if err := db.Close(); err != nil {
		a.Err = err
		return
	}
	close(res)
	a.Active = false
}

func exec(ctx context.Context, distSqls []string, times int, res chan bool) {
	for i := 0; i < times; i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if len(distSqls) == 0 {
			return
		}
		for i := range distSqls {
			_, err := db.Exec(distSqls[i])
			res <- err == nil
		}
	}
}

func handleResponse(ctx context.Context, res chan bool, result *Result) {
	start := time.Now()
For:
	for {
		select {
		case <-ctx.Done():
			break For
		case ret := <-res:
			//todo: add more msg
			handle(ret, result)
		}
	}
	for totalReq > result.Total {
		val := <-res
		handle(val, result)
	}

	end := time.Now()
	result.Duration = end.Sub(start)
}

//todo:add more logic and change ret type(bool ---> struct)
func handle(ret bool, result *Result) {
	if ret {
		result.Success++
	}
	result.Total++
}
