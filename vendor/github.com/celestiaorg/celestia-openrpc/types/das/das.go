package das

// SamplingStats collects information about the DASer process.
type SamplingStats struct {
	// all headers before SampledChainHead were successfully sampled
	SampledChainHead uint64 `json:"head_of_sampled_chain"`
	// all headers before CatchupHead were submitted to sampling workers. They could be either already
	// sampled, failed or still in progress. For in progress items check Workers stat.
	CatchupHead uint64 `json:"head_of_catchup"`
	// NetworkHead is the height of the most recent header in the network
	NetworkHead uint64 `json:"network_head_height"`
	// Failed contains all skipped headers heights with corresponding try count
	Failed map[uint64]int `json:"failed,omitempty"`
	// Workers has information about each currently running worker stats
	Workers []WorkerStats `json:"workers,omitempty"`
	// Concurrency amount of currently running parallel workers
	Concurrency int `json:"concurrency"`
	// CatchUpDone indicates whether all known headers are sampled
	CatchUpDone bool `json:"catch_up_done"`
	// IsRunning tracks whether the DASer service is running
	IsRunning bool `json:"is_running"`
}

type WorkerStats struct {
	JobType jobType `json:"job_type"`
	Curr    uint64  `json:"current"`
	From    uint64  `json:"from"`
	To      uint64  `json:"to"`

	ErrMsg string `json:"error,omitempty"`
}

type jobType string
