package scheduling

// Status: undefined state
const BasicCallbackStatusUndefined = "UNDEFINED"

// Status: completed state
const BasicCallbackStatusComplete = "COMPLETE"

// Status: failed state. `Result` contains error
const BasicCallbackStatusFail = "FAIL"

// Status: error state. `Result` contains error. Retried until MaxRetries
const BasicCallbackStatusError = "ERROR"

// Status: task cancelled. May conrain error in `Result`
const BasicCallbackStatusCancel = "CANCEL"

// Task processing result
type BasicCallbackResult struct {
	// Task status
	Status string `json:"status"`

	// Optional result
	Result interface{} `json:"result"`
}
