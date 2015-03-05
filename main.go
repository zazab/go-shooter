package shooter

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"sort"
	"sync"
	"time"
)

type Request interface {
	Prepare()
	Launch(string, chan string)
	GetTime() time.Duration
	GetStart() time.Time
}

var data map[int][]time.Duration = map[int][]time.Duration{}

type Shooter struct {
	aggr        float64
	prepared    []Request
	prepMut     *sync.Mutex
	launched    map[string]Request
	launchMut   sync.Mutex
	prepChan    chan Request
	doneChan    chan string
	statChan    chan float64
	flushChan   chan os.Signal
	PrepareFunc func() Request
	wg          sync.WaitGroup
}

func NewShooter() *Shooter {
	sh := &Shooter{
		aggr:      1,
		prepared:  []Request{},
		prepMut:   &sync.Mutex{},
		launched:  map[string]Request{},
		launchMut: sync.Mutex{},
		prepChan:  make(chan Request, 1000),
		doneChan:  make(chan string, 100),
		statChan:  make(chan float64, 10),
		flushChan: make(chan os.Signal, 1),
		wg:        sync.WaitGroup{},
	}

	signal.Notify(sh.flushChan, os.Interrupt, os.Kill)
	return sh
}

func (s *Shooter) launchPrepareres(rps int64) {
	go s.prepareThread(rps)

	n := len(s.prepared)
	time.Sleep(1 * time.Second)
	dn := len(s.prepared) - n
	needPreparers := int(float64(rps)*1.2)/dn + 1

	for i := 0; i < needPreparers; i++ {
		go s.prepareThread(rps)
	}
	log.Printf("launched %d preparers\n", needPreparers+1)
}

func (s *Shooter) Run(rps int64, threads int64) {
	s.wg.Add(3)
	s.launchWatchers(rps)
	s.launchPrepareres(rps)
	s.launchFireThreads(rps, threads)
	s.wg.Wait()
}

func (s *Shooter) launchFireThreads(rps, threads int64) {
	oneRps := rps / threads
	for i := int64(0); i < threads; i++ {
		go s.fireThread(oneRps)
	}
}

func (s *Shooter) fireThread(rps int64) {
	launched := int64(0)
	started := time.Now()
	var req Request
	for {
		//log.Println("fire")
		s.prepMut.Lock()
		if len(s.prepared) > 0 {
			req = s.prepared[0]
			s.prepared = s.prepared[1:]
		} else {
			s.prepMut.Unlock()
			time.Sleep(50 * time.Millisecond)
			continue
		}
		s.prepMut.Unlock()
		id := fmt.Sprintf("%x", time.Now().UnixNano())
		go req.Launch(id, s.doneChan)
		s.launchMut.Lock()
		s.launched[id] = req
		s.launchMut.Unlock()
		launched++

		elapsed := time.Now().Sub(started).Seconds()
		if elapsed >= s.aggr {
			s.statChan <- float64(launched) / elapsed
			launched = 0
			started = time.Now()
			continue
		}

		st := int64((s.aggr - elapsed) / (s.aggr * float64(rps-launched)) * 1000000)
		time.Sleep(time.Duration(st) * time.Microsecond)
	}
}

func (s *Shooter) launchWatchers(rps int64) {
	go s.doneWatcher(rps)
	go s.prepareWatcher()
}

func (s *Shooter) prepareWatcher() {
	if s.PrepareFunc == nil {
		panic("prepare func not set")
	}

	for {
		req := <-s.prepChan
		s.prepMut.Lock()
		s.prepared = append(s.prepared, req)
		s.prepMut.Unlock()

	}
}

func (s *Shooter) doneWatcher(rps int64) {
	f, err := os.OpenFile("./stats.csv", os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	f.Truncate(0)
	for {
		select {
		case id := <-s.doneChan:
			st := s.launched[id].GetStart()
			sec := st.Second()
			t := s.launched[id].GetTime()
			s.launchMut.Lock()
			delete(s.launched, id)
			s.launchMut.Unlock()
			if data[sec] == nil {
				data[sec] = []time.Duration{}
			}
			data[sec] = append(data[sec], t)
			if cnt := int64(len(data[sec])); cnt >= rps {
				r, avg, p80, p90 := printSecStatistics(sec)
				log.Printf("rps: %d avg: %6.2f p50: %6.2f p85: %6.2f\n", r, avg, p80, p90)
				f.WriteString(fmt.Sprintf("%d;%d;%.2f;%.2f;%.2f\n", sec, r, avg, p80, p90))
				delete(data, sec)
			}
		case <-s.statChan:
		case <-s.flushChan:
			f.Close()
			os.Exit(0)
		}
	}
}

type sortInt64 []int64

func (v sortInt64) Len() int      { return len(v) }
func (v sortInt64) Swap(i, j int) { v[i], v[j] = v[j], v[i] }
func (v sortInt64) Less(i, j int) bool {
	return v[i] < v[j]
}

func calcPerc(times []time.Duration, percs ...float64) []int64 {
	buf := make([]int64, len(times))
	for i, v := range times {
		buf[i] = int64(v.Nanoseconds() / 1000)
	}

	sort.Sort(sortInt64(buf))
	res := make([]int64, len(percs))
	for i, p := range percs {
		res[i] = buf[int64(p*float64(len(times)))]
	}

	return res
}

func printSecStatistics(sec int) (int64, float64, float64, float64) {
	avg := int64(0)
	cnt := len(data[sec])
	for _, d := range data[sec] {
		avg += d.Nanoseconds() / 1000
	}
	avg /= int64(cnt)
	percs := calcPerc(data[sec], 0.5, 0.85)

	return int64(cnt), float64(avg) / 1000, float64(percs[0]) / 1000, float64(percs[1]) / 1000
}

func (s *Shooter) prepareThread(rps int64) {
	for {
		s.prepMut.Lock()
		l := len(s.prepared)
		s.prepMut.Unlock()
		if int64(l) < rps {
			r := s.PrepareFunc()
			s.prepChan <- r
		}

		time.Sleep(50 * time.Millisecond)
	}
}
