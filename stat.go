// Copyright 2022 Ed Huang<i@huangdx.net>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tipubsub

import (
	"encoding/json"
	"sync"
)

type Stat struct {
	mu sync.RWMutex
	m  map[string]interface{}
}

var sysStat *Stat

func init() {
	sysStat = new(Stat)
}

func GetStat() *Stat {
	return sysStat
}

func (s *Stat) Set(key string, val interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[key] = val
}

func (s *Stat) Inc(key string, val int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if v, ok := s.m[key]; ok {
		if v, ok := v.(int); ok {
			s.m[key] = v + val
		}
	} else {
		s.m[key] = val
	}
}

func (s *Stat) JSON() (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, err := json.Marshal(s.m)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
