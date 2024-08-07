package worker

import (
	"crivoe/scheduling"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/bitrate16/bloby"
)

type UrlWorker struct {
	storage bloby.Storage
}

func NewUrlWorker(storage bloby.Storage) *UrlWorker {
	return &UrlWorker{
		storage: storage,
	}
}

// Oneshot url downloader
func (w *UrlWorker) Launch(task *scheduling.BasicTask, callback scheduling.BasicCallback) {
	var result scheduling.BasicCallbackResult

	if task == nil {
		result.Status = scheduling.BasicCallbackStatusError
		result.Result = errors.New("task is nil")
		callback.Done(&result)
		return
	}

	go func() {
		method := tryGetString(task.Options, "method", "GET")
		url := tryGetString(task.Options, "url", "")
		timeout := tryGetUInt64(task.Options, "timeout", 10000)
		timestamp := time.Now()

		client := &http.Client{}
		req, err := http.NewRequest(method, url, nil)
		if err != nil {
			result.Status = scheduling.BasicCallbackStatusError
			result.Result = err
			callback.Done(&result)
			return
		}
		client.Timeout = time.Duration(timeout) * time.Millisecond

		resp, err := client.Do(req)
		if err != nil {
			result.Status = scheduling.BasicCallbackStatusError
			result.Result = err
			callback.Done(&result)
			return
		}
		defer resp.Body.Close()

		// Save headers
		headers := make(map[string][]string)
		for k, v := range resp.Header {
			headers[k] = v
		}

		// Create metadata
		metadata := make(map[string]interface{})
		metadata["id"] = task.Id
		metadata["options"] = task.Options
		metadata["timestamp"] = timestamp
		metadata["headers"] = headers
		metadata["method"] = method
		metadata["url"] = url
		metadata["status"] = "undefined"

		// TODO: Do not delete node, but update metadata on errors and/or success
		// var node bloby.Node
		// if exists, err := w.storage.ExistsByName(task.Id); exists {
		// 	n, err := w.storage.GetByReference(task.Id)
		// 	if err != nil {
		// 		result.Status = scheduling.BasicCallbackStatusError
		// 		result.Result = err
		// 		callback.Done(&result)
		// 		return
		// 	}
		// }
		node, err := w.storage.Create(
			task.Id,
			metadata,
		)
		if err != nil {
			result.Status = scheduling.BasicCallbackStatusError
			result.Result = err
			callback.Done(&result)
			return
		}

		if writable, ok := node.(bloby.Writable); ok {

			writer, err := writable.GetWriter()
			if err != nil {
				// Delete created node
				err := w.storage.Delete(node.GetReference())
				if err != nil {
					result.Status = scheduling.BasicCallbackStatusError
					result.Result = err
					callback.Done(&result)
					return
				}

				result.Status = scheduling.BasicCallbackStatusError
				result.Result = err
				callback.Done(&result)
				return
			}

			_, err = io.Copy(writer, resp.Body)
			if err != nil {
				// Delete created node
				err := w.storage.Delete(node.GetReference())
				if err != nil {
					result.Status = scheduling.BasicCallbackStatusError
					result.Result = err
					callback.Done(&result)
					return
				}

				result.Status = scheduling.BasicCallbackStatusError
				result.Result = err
				callback.Done(&result)
				return
			}

			if mutable, ok := node.(bloby.Mutable); ok {
				metadata["status"] = "done"
				mutable.SetMetadata(metadata)
			}

			if closer, ok := writer.(io.Closer); ok {
				closer.Close()
			}

			result.Status = scheduling.BasicCallbackStatusComplete
			result.Result = nil
			callback.Done(&result)

		} else {
			// Delete created node
			err := w.storage.Delete(node.GetReference())
			if err != nil {
				result.Status = scheduling.BasicCallbackStatusError
				result.Result = err
				callback.Done(&result)
				return
			}

			result.Status = scheduling.BasicCallbackStatusError
			result.Result = errors.New("storage not writable")
			callback.Done(&result)
			return
		}
	}()
}
