package pupupu

// Status: undefined state
const StatusUndefined = "UNDEFINED"

// Status: completed state
const StatusComplete = "COMPLETE"

// Status: failed state. `Result` contains error
const StatusFail = "FAIL"

// Status: error state. `Result` contains error. Retried until MaxRetries
const StatusError = "ERROR"

// Status: task cancelled. May conrain error in `Result`
const StatusCancel = "CANCEL"
