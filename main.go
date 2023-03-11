package main

import (
	//"container/list"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"strconv"
	"strings"

	"github.com/olekukonko/tablewriter"
)

func main() {
	// CLI args
	f, closeFile, err := openProcessingFile(os.Args...)
	if err != nil {
		log.Fatal(err)
	}
	defer closeFile()

	// Load and parse processes
	processes, err := loadProcesses(f)
	if err != nil {
		log.Fatal(err)
	}

	// First-come, first-serve scheduling
	FCFSSchedule(os.Stdout, "First-come, first-serve", processes)

	SJFSchedule(os.Stdout, "Shortest-job-first", processes)
	//
	SJFPrioritySchedule(os.Stdout, "Priority", processes)
	//
	RRSchedule(os.Stdout, "Round-robin", processes)
}

func openProcessingFile(args ...string) (*os.File, func(), error) {
	if len(args) != 2 {
		return nil, nil, fmt.Errorf("%w: must give a scheduling file to process", ErrInvalidArgs)
	}
	// Read in CSV process CSV file
	f, err := os.Open(args[1])
	if err != nil {
		return nil, nil, fmt.Errorf("%v: error opening scheduling file", err)
	}
	closeFn := func() {
		if err := f.Close(); err != nil {
			log.Fatalf("%v: error closing scheduling file", err)
		}
	}

	return f, closeFn, nil
}

type (
	Process struct {
		ProcessID     int64
		ArrivalTime   int64
		BurstDuration int64
		Priority      int64
		cTime         int64
		tTime         int64
		wTime         int64
		sTime         int64
	}
	TimeSlice struct {
		PID   int64
		Start int64
		Stop  int64
	}
)

//region Schedulers

// FCFSSchedule outputs a schedule of processes in a GANTT chart and a table of timing given:
// • an output writer
// • a title for the chart
// • a slice of processes
func FCFSSchedule(w io.Writer, title string, processes []Process) {
	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
	)

	for i := range processes {
		if processes[i].ArrivalTime > 0 {
			waitingTime = serviceTime - processes[i].ArrivalTime

		}

		totalWait += float64(waitingTime)

		start := waitingTime + processes[i].ArrivalTime

		turnaround := processes[i].BurstDuration + waitingTime
		totalTurnaround += float64(turnaround)

		completion := processes[i].BurstDuration + processes[i].ArrivalTime + waitingTime
		lastCompletion = float64(completion)

		schedule[i] = []string{
			fmt.Sprint(processes[i].ProcessID),
			fmt.Sprint(processes[i].Priority),
			fmt.Sprint(processes[i].BurstDuration),
			fmt.Sprint(processes[i].ArrivalTime),
			fmt.Sprint(waitingTime),
			fmt.Sprint(turnaround),
			fmt.Sprint(completion),
		}
		serviceTime += processes[i].BurstDuration

		gantt = append(gantt, TimeSlice{
			PID:   processes[i].ProcessID,
			Start: start,
			Stop:  serviceTime,
		})
	}

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

func SJFPrioritySchedule(w io.Writer, title string, processes []Process) {

	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		currTime        int64 = 0
		n               int64 = int64(len(processes))
		hp              int64 = math.MaxInt64
		minm            int64 = math.MaxInt64
		complete        int64 = 0
		highest         int   = 0
		check           bool  = false
		rt                    = make([]int64, len(processes))
		schedule              = make([][]string, len(processes))
		gantt                 = make([]TimeSlice, 0)
		lastStart       int64 = 0
	)

	for i := range processes {
		rt[i] = processes[i].BurstDuration
	}

	for complete != n {
		for i := range processes {
			if (processes[i].ArrivalTime <= currTime) && (rt[i] > 0) &&
				(processes[i].Priority < hp || processes[i].Priority == hp) {
				if processes[i].Priority != hp {
					hp = processes[i].Priority
					highest = i
					check = true
				}
				if processes[i].Priority == hp {
					bd1 := processes[i].BurstDuration
					bd2 := processes[highest].BurstDuration

					if bd1 > bd2 {
						check = true
					}
					if bd1 < bd2 {
						highest = i
						check = true
					}
				}
			}
		}

		if !check {

			currTime++
			continue
		}

		rt[highest]--
		minm = rt[highest]

		if minm == 0 {
			hp = math.MaxInt64
			minm = math.MaxInt64
		}

		if rt[highest] == 0 {

			complete++
			check = false

			serviceTime = currTime + 1

			// Calculate waiting time
			waitingTime = serviceTime - processes[highest].BurstDuration - processes[highest].ArrivalTime

			if waitingTime < 0 {
				waitingTime = 0
			}

			totalWait += float64(waitingTime)

			//start := waitingTime + processes[highest].ArrivalTime

			turnaround := processes[highest].BurstDuration + waitingTime
			totalTurnaround += float64(turnaround)

			completion := processes[highest].BurstDuration + processes[highest].ArrivalTime + waitingTime

			lastCompletion = float64(completion)

			schedule[highest] = []string{
				fmt.Sprint(processes[highest].ProcessID),
				fmt.Sprint(processes[highest].Priority),
				fmt.Sprint(processes[highest].BurstDuration),
				fmt.Sprint(processes[highest].ArrivalTime),
				fmt.Sprint(waitingTime),
				fmt.Sprint(turnaround),
				fmt.Sprint(completion),
			}

			gantt = append(gantt, TimeSlice{
				PID:   processes[highest].ProcessID,
				Start: lastStart,
				Stop:  serviceTime,
			})

		}

		currTime++
		lastStart = serviceTime
	}

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)

}

func SJFSchedule(w io.Writer, title string, processes []Process) {
	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		complete        int64 = 0
		n               int64 = int64(len(processes))
		currTime        int64 = 0
		minm            int64 = math.MaxInt64
		shortest        int   = 0
		check           bool  = false
		rt                    = make([]int64, len(processes))
		schedule              = make([][]string, len(processes))
		gantt                 = make([]TimeSlice, 0)
		lastStart       int64 = 0
	)

	for i := range processes {
		rt[i] = processes[i].BurstDuration
	}

	for complete != n {

		for i := range processes {
			if (processes[i].ArrivalTime <= currTime) && (rt[i] < minm) && (rt[i] > 0) {
				minm = rt[i]
				shortest = i
				check = true

			}
		}

		if !check {

			currTime++
			continue
		}

		rt[shortest]--
		minm = rt[shortest]

		if minm == 0 {
			minm = math.MaxInt64
		}

		if rt[shortest] == 0 {

			// Increment complete
			complete++
			check = false

			serviceTime = currTime + 1

			// Calculate waiting time
			waitingTime = serviceTime - processes[shortest].BurstDuration - processes[shortest].ArrivalTime

			if waitingTime < 0 {
				waitingTime = 0
			}

			totalWait += float64(waitingTime)

			//start := waitingTime + processes[shortest].ArrivalTime

			turnaround := processes[shortest].BurstDuration + waitingTime
			totalTurnaround += float64(turnaround)

			completion := processes[shortest].BurstDuration + processes[shortest].ArrivalTime + waitingTime

			lastCompletion = float64(completion)

			schedule[shortest] = []string{
				fmt.Sprint(processes[shortest].ProcessID),
				fmt.Sprint(processes[shortest].Priority),
				fmt.Sprint(processes[shortest].BurstDuration),
				fmt.Sprint(processes[shortest].ArrivalTime),
				fmt.Sprint(waitingTime),
				fmt.Sprint(turnaround),
				fmt.Sprint(completion),
			}

			gantt = append(gantt, TimeSlice{
				PID:   processes[shortest].ProcessID,
				Start: lastStart,
				Stop:  serviceTime,
			})

		}

		currTime++
		lastStart = serviceTime
	}

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)

}

func RRSchedule(w io.Writer, title string, processes []Process) {
	var (
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		rt                    = make([]int64, len(processes))
		burstArr              = make([]int64, len(processes))
		currTime        int64 = 0
		qauntum         int64 = 5
		complete        int64 = 0
		n               int64 = int64(len(processes))
		schedule              = make([][]string, len(processes))
		gantt                 = make([]TimeSlice, 0)
		idx             int64
		q               []int64
		mark                  = make([]int, 100)
		serviceTime     int64 = 0
		lastStart       int64 = 0
	)

	mark[0] = 1
	q = append(q, 0)

	for i := range processes {
		rt[i] = processes[i].BurstDuration
		burstArr[i] = processes[i].BurstDuration

	}

	for complete != n {

		idx = (q[0])
		q[0] = 0
		q = q[1:]

		if burstArr[idx] == processes[idx].BurstDuration {
			processes[idx].sTime = int64(math.Max(float64(currTime), float64(processes[idx].ArrivalTime)))
			currTime = processes[idx].sTime

		}

		if 0 < burstArr[idx]-qauntum {
			burstArr[idx] -= qauntum
			currTime += qauntum

			serviceTime += qauntum

		} else {
			currTime += burstArr[idx]
			processes[idx].cTime = currTime
			processes[idx].tTime = processes[idx].cTime - processes[idx].ArrivalTime
			processes[idx].wTime = processes[idx].tTime - processes[idx].BurstDuration
			totalWait += float64(processes[idx].wTime)
			totalTurnaround += float64(processes[idx].tTime)
			complete++
			serviceTime += burstArr[idx]
			burstArr[idx] = 0

			completion := processes[idx].BurstDuration + processes[idx].ArrivalTime + processes[idx].wTime
			lastCompletion = float64(completion)

			schedule[idx] = []string{
				fmt.Sprint(processes[idx].ProcessID),
				fmt.Sprint(processes[idx].Priority),
				fmt.Sprint(processes[idx].BurstDuration),
				fmt.Sprint(processes[idx].ArrivalTime),
				fmt.Sprint(processes[idx].wTime),
				fmt.Sprint(processes[idx].tTime),
				fmt.Sprint(completion),
			}
		}

		for i := range processes {

			if burstArr[i] > 0 && processes[i].ArrivalTime <= currTime && mark[i] == 0 {
				mark[i] = 1
				q = append(q, int64(i))

			}
		}

		if 0 < burstArr[idx] {
			q = append(q, idx)
		}

		if q == nil {
			for i := range processes {
				if 0 < burstArr[i] {
					mark[i] = 1
					q = append(q, int64(i))

					break
				}
			}
		}

		gantt = append(gantt, TimeSlice{
			PID:   processes[idx].ProcessID,
			Start: lastStart,
			Stop:  serviceTime,
		})

		lastStart = serviceTime

	}

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)

}

//endregion

//region Output helpers

func outputTitle(w io.Writer, title string) {
	_, _ = fmt.Fprintln(w, strings.Repeat("-", len(title)*2))
	_, _ = fmt.Fprintln(w, strings.Repeat(" ", len(title)/2), title)
	_, _ = fmt.Fprintln(w, strings.Repeat("-", len(title)*2))
}

func outputGantt(w io.Writer, gantt []TimeSlice) {
	_, _ = fmt.Fprintln(w, "Gantt schedule")
	_, _ = fmt.Fprint(w, "|")
	for i := range gantt {
		pid := fmt.Sprint(gantt[i].PID)
		padding := strings.Repeat(" ", (8-len(pid))/2)
		_, _ = fmt.Fprint(w, padding, pid, padding, "|")
	}
	_, _ = fmt.Fprintln(w)
	for i := range gantt {
		_, _ = fmt.Fprint(w, fmt.Sprint(gantt[i].Start), "\t")
		if len(gantt)-1 == i {
			_, _ = fmt.Fprint(w, fmt.Sprint(gantt[i].Stop))
		}
	}
	_, _ = fmt.Fprintf(w, "\n\n")
}

func outputSchedule(w io.Writer, rows [][]string, wait, turnaround, throughput float64) {
	_, _ = fmt.Fprintln(w, "Schedule table")
	table := tablewriter.NewWriter(w)
	table.SetHeader([]string{"ID", "Priority", "Burst", "Arrival", "Wait", "Turnaround", "Exit"})
	table.AppendBulk(rows)
	table.SetFooter([]string{"", "", "", "",
		fmt.Sprintf("Average\n%.2f", wait),
		fmt.Sprintf("Average\n%.2f", turnaround),
		fmt.Sprintf("Throughput\n%.2f/t", throughput)})
	table.Render()
}

//endregion

//region Loading processes.

var ErrInvalidArgs = errors.New("invalid args")

func loadProcesses(r io.Reader) ([]Process, error) {
	rows, err := csv.NewReader(r).ReadAll()
	if err != nil {
		return nil, fmt.Errorf("%w: reading CSV", err)
	}

	processes := make([]Process, len(rows))
	for i := range rows {
		processes[i].ProcessID = mustStrToInt(rows[i][0])
		processes[i].BurstDuration = mustStrToInt(rows[i][1])
		processes[i].ArrivalTime = mustStrToInt(rows[i][2])
		if len(rows[i]) == 4 {
			processes[i].Priority = mustStrToInt(rows[i][3])
		}
	}

	return processes, nil
}

func mustStrToInt(s string) int64 {
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	return i
}

//endregion
